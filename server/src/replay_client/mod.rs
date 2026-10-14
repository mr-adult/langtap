use std::{
    collections::{HashMap, HashSet, VecDeque},
    convert::Infallible,
    io::BufReader,
    string::FromUtf8Error,
    task::Waker,
    time::Duration,
    vec::IntoIter,
};

use axum::{
    extract::{Query, State, ws::Message as AxumWsMessage},
    http::{HeaderMap, StatusCode},
    response::Redirect,
};
use futures::{Sink, Stream};
use lsp_types::{
    NumberOrString,
    notification::{Exit, Notification as LspNotification},
    request::{Request as LspRequest, Shutdown},
};
use serde::Deserialize;
use serde_json::Value;
use time::OffsetDateTime;
use tracing::{error, info_span};

use crate::{
    AppState,
    lsp::{self},
    message::{
        Conversation, ErrorCode, Message as LspMessage, MessageKind, Notification, Request,
        RequestId, Response,
    },
    session::{MessageSource, MessageWithTimeStamp},
};

#[derive(Deserialize)]
pub(crate) struct ReplayRequest {
    session_id: i64,
    termination_message: Option<i64>,
    message_kind: Option<u8>,
}

pub async fn handle_replay(
    State(app_state): State<AppState>,
    Query(ReplayRequest {
        session_id,
        termination_message,
        message_kind,
    }): Query<ReplayRequest>,
) -> Result<Redirect, StatusCode> {
    let message_kind = if let Some(message_kind) = message_kind {
        Some(MessageKind::try_from(message_kind).map_err(|_| StatusCode::BAD_REQUEST)?)
    } else {
        None
    };

    let conversation = crate::session::get_all_messages_for_session_in_chronological_order(
        &app_state.db,
        session_id,
    )
    .await
    .map_err(|err| match err {
        sqlx::Error::RowNotFound => StatusCode::NOT_FOUND,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    })?;

    let session_id = lsp::start_session(&app_state.db)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let session_span = info_span!("session_id", session_id = session_id);
    let _session_span_handle: tracing::span::Entered<'_> = session_span.enter();

    let client_stream = Messages::new(conversation, termination_message, message_kind);
    let server_socket =
        lsp::connect_to_server((*app_state.forward_url).clone(), HeaderMap::default())
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .0;

    match lsp::run_session_with_client(session_id, client_stream, server_socket, app_state).await {
        Err(()) => Err(StatusCode::INTERNAL_SERVER_ERROR),
        Ok(session_id) => Ok(Redirect::to(&format!("/session?session_id={}", session_id))),
    }
}

struct Messages {
    stream_waker: Option<Waker>,
    current_group: Option<(IntoIter<MessageWithTimeStamp>, OffsetDateTime)>,
    server_responses: VecDeque<LspMessage>,
    current_group_finished: bool,
    messages: IntoIter<Vec<MessageWithTimeStamp>>,
    size_hint: usize,
    requests: HashMap<RequestId, Request>,
    progress_tokens: HashMap<NumberOrString, Request>,
    termination_message: Option<i64>,
    done: bool,
    outbound_requests: HashSet<RequestId>,
}

impl Messages {
    fn new(
        value: Conversation,
        termination_message_id: Option<i64>,
        message_kind: Option<MessageKind>,
    ) -> Self {
        // FUTURE: some requests can be processed concurrently, but the logic
        // around it is convoluted. For now just run everything serially.
        // Examples of convoluted logic:
        // - all text document and notebook document synchronization messages
        // must be run serially and block other requests
        // - call hierarchy must have the prepare request resolve before the
        // incoming/outgoing calls requests are sent off
        let mut concurrent_message_groups = Vec::new();

        for message_with_timestamp in value.messages {
            if message_with_timestamp.source != MessageSource::Client {
                continue;
            }
            match &message_with_timestamp.message {
                // FUTURE: handle Responses. This is complicated because we
                // can't be sure that the server will deterministically ask
                // for the same things at the same times.
                LspMessage::Response(_) => continue,
                LspMessage::Request(_) | LspMessage::Notification(_) => {}
            }

            let db_id = message_with_timestamp.db_id;
            let message_with_timestamp_kind = message_with_timestamp.message.kind();
            concurrent_message_groups.push(vec![message_with_timestamp]);

            if let Some(termination_message_id) = termination_message_id
                && termination_message_id == db_id
            {
                if let Some(message_kind) = message_kind
                    && message_kind == message_with_timestamp_kind
                {
                    break;
                }
            }
        }

        Self {
            stream_waker: None,
            current_group: None,
            current_group_finished: true,
            server_responses: VecDeque::new(),
            size_hint: concurrent_message_groups.len(),
            messages: concurrent_message_groups.into_iter(),
            requests: value.requests,
            progress_tokens: value.progress_tokens,
            termination_message: None,
            done: false,
            outbound_requests: HashSet::new(),
        }
    }
}

impl Stream for Messages {
    type Item = Result<AxumWsMessage, Infallible>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if let Some(response) = self.server_responses.pop_front() {
            let mut msg_bytes = Vec::new();
            response
                .write(&mut msg_bytes)
                .expect("writing to a vec to never fail");
            let msg = AxumWsMessage::Text(msg_bytes.try_into().expect("bytes to be UTF-8 text"));
            return std::task::Poll::Ready(Some(Ok(msg)));
        }

        if let Some((current_group, _)) = &mut self.current_group {
            if let Some(item) = current_group.next() {
                match &item.message {
                    LspMessage::Request(request) => {
                        self.outbound_requests.insert(request.id.clone());
                        self.current_group_finished = false;
                    }
                    LspMessage::Response(_) => {}
                    LspMessage::Notification(_) => {}
                }

                let mut msg_bytes = Vec::new();
                item.message
                    .write(&mut msg_bytes)
                    .expect("writing to a vec to never fail");
                let msg =
                    AxumWsMessage::Text(msg_bytes.try_into().expect("bytes to be UTF-8 text"));
                return std::task::Poll::Ready(Some(Ok(msg)));
            }

            if self.outbound_requests.is_empty() {}
        }

        if !self.current_group_finished {
            self.stream_waker = Some(cx.waker().clone());
            return std::task::Poll::Pending;
        }

        if self.done {
            return std::task::Poll::Ready(None);
        }

        if let Some(message_group) = self.messages.next() {
            self.current_group = Some((message_group.into_iter(), OffsetDateTime::now_utc()));

            if let Some(message_with_timestamp) = self
                .current_group
                .as_mut()
                .map(|tuple| &mut tuple.0)
                .expect("current group to have just been set")
                .next()
            {
                match &message_with_timestamp.message {
                    LspMessage::Request(request) => {
                        self.outbound_requests.insert(request.id.clone());
                        self.current_group_finished = false;
                    }
                    LspMessage::Response(_) => {}
                    LspMessage::Notification(_) => {}
                }

                let mut msg_bytes = Vec::new();
                LspMessage::write(&message_with_timestamp.message, &mut msg_bytes)
                    .expect("writing to a vec to never fail");
                let msg_utf8_bytes = msg_bytes.try_into().expect("bytes to be valid UTF-8");
                let axum_msg = AxumWsMessage::Text(msg_utf8_bytes);

                let result = Ok(axum_msg);
                return std::task::Poll::Ready(Some(result));
            }
        }

        std::task::Poll::Ready(None)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.size_hint))
    }
}

impl Sink<AxumWsMessage> for Messages {
    type Error = FromUtf8Error;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn start_send(
        mut self: std::pin::Pin<&mut Self>,
        item: AxumWsMessage,
    ) -> Result<(), Self::Error> {
        if !self.current_group_finished {
            match &self.current_group {
                None => {
                    self.current_group_finished = true;
                }
                Some((_, start_time)) => {
                    // time out the connection after 2 mins
                    let comparison_time = OffsetDateTime::now_utc() - Duration::from_secs(120);
                    if *start_time < comparison_time {
                        // clear out the remaining messages
                        self.messages = Vec::new().into_iter();

                        self.server_responses
                            .push_back(LspMessage::Request(Request::new(
                                RequestId::from(i32::MAX - 1),
                                Shutdown::METHOD.to_string(),
                                (),
                            )));

                        self.server_responses
                            .push_back(LspMessage::Notification(Notification {
                                method: Exit::METHOD.to_string(),
                                params: Value::Null,
                            }));
                    }
                }
            }
        }

        let bytes = match item {
            AxumWsMessage::Binary(bytes) => {
                let bytes: Vec<u8> = bytes.into();
                String::try_from(bytes)?
            }
            AxumWsMessage::Text(text) => text.as_str().to_string(),
            AxumWsMessage::Close(_) => {
                self.done = true;
                return Ok(());
            }
            AxumWsMessage::Ping(_) | AxumWsMessage::Pong(_) => {
                return Ok(());
            }
        };

        let mut buf_reader = BufReader::new(bytes.as_bytes());
        let msg = match LspMessage::read(&mut buf_reader) {
            Err(_) => unreachable!("strings should never emit io::Errors during reading"),
            Ok(msg) => msg,
        };

        if let Some(msg) = msg {
            match msg {
                LspMessage::Request(request) => {
                    self.server_responses
                        .push_back(LspMessage::Response(Response::new_err(
                            request.id,
                            ErrorCode::RequestFailed as i32,
                            "LSP Replay client was unable to service this request.".to_string(),
                        )));
                }
                LspMessage::Response(response) => {
                    self.outbound_requests.remove(&response.id);
                    if self.outbound_requests.is_empty() {
                        self.current_group_finished = true;
                        if let Some(waker) = self.stream_waker.take() {
                            waker.wake();
                        }
                    }
                }
                LspMessage::Notification(_notification) => {}
            }
        } else {
            error!("Failed to read LspMessage");
        }

        Ok(())
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
}
