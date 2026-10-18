use std::{fmt::Display, io::BufReader, sync::Arc, time::Duration};

use axum::extract::ws::Message as AxumWsMessage;
use futures::{
    Sink, SinkExt, Stream, StreamExt,
    stream::{SplitSink, SplitStream},
};
use lsp_types::{
    InitializeParams, InitializeResult, InitializedParams,
    notification::{Initialized, Notification as LspNotification},
    request::{Initialize, Request as LspRequest, ShowMessageRequest},
};
use time::OffsetDateTime;
use tokio::sync::{Mutex, RwLock};
use tokio_tungstenite::tungstenite::Message as TokioWsMessage;
use tracing::{Instrument, error, warn};

use crate::{
    lsp::{LspSession, Shutdown, Uninitialized},
    message::{ErrorCode, Message as LspMessage, MessageKind, Notification, Request, Response},
    session::{ExpectedSender, MessageSource},
};

pub(crate) async fn run_to_exit<'session, ServerStream, ClientStream, ClientError>(
    source: SplitStream<ClientStream>,
    server_sink: Arc<Mutex<SplitSink<ServerStream, TokioWsMessage>>>,
    client_sink: Arc<Mutex<SplitSink<ClientStream, AxumWsMessage>>>,
    shared_state: Arc<RwLock<Uninitialized>>,
    session: &'session LspSession<'session>,
) where
    ClientStream: Stream<Item = Result<AxumWsMessage, ClientError>> + Sink<AxumWsMessage> + Unpin,
    ClientStream::Error: Display,
    ClientError: Display,
    ServerStream: Sink<TokioWsMessage> + Unpin,
    ServerStream::Error: Display,
{
    match ClientToServerProxy::<
        'session,
        Arc<RwLock<Uninitialized>>,
        ServerStream,
        ClientStream,
        ClientError,
    >::new(source, server_sink, client_sink, shared_state, session)
    .initialize()
    .await
    {
        Ok(initialized) => match initialized.run_proxy().await {
            Ok(shutdown) => shutdown.listen_for_exit().await,
            Err(()) => {}
        },
        Err(()) => {}
    };
}

pub(crate) struct ClientToServerProxy<'session, State, ServerStream, ClientStream, ClientError>
where
    ClientStream: Stream<Item = Result<AxumWsMessage, ClientError>> + Sink<AxumWsMessage> + Unpin,
    ClientStream::Error: Display,
    ClientError: Display,
    ServerStream: Sink<TokioWsMessage> + Unpin,
    ServerStream::Error: Display,
{
    source: SplitStream<ClientStream>,
    server_sink: Arc<Mutex<SplitSink<ServerStream, TokioWsMessage>>>,
    client_sink: Arc<Mutex<SplitSink<ClientStream, AxumWsMessage>>>,
    state: State,
    session: &'session LspSession<'session>,
}

impl<'session, State, ServerStream, ClientStream, ClientError>
    ClientToServerProxy<'session, State, ServerStream, ClientStream, ClientError>
where
    ClientStream: Stream<Item = Result<AxumWsMessage, ClientError>> + Sink<AxumWsMessage> + Unpin,
    ClientStream::Error: Display,
    ClientError: Display,
    ServerStream: Sink<TokioWsMessage> + Unpin,
    ServerStream::Error: Display,
{
    fn parse_message(message: AxumWsMessage) -> Result<Option<LspMessage>, ()> {
        let lsp_message_bytes = match &message {
            AxumWsMessage::Text(utf8_bytes) => utf8_bytes.as_bytes(),
            AxumWsMessage::Binary(bytes) => &bytes,
            AxumWsMessage::Ping(_) | AxumWsMessage::Pong(_) => return Ok(None),
            AxumWsMessage::Close(_) => return Ok(None),
        };

        match LspMessage::read(&mut BufReader::new(lsp_message_bytes)) {
            Err(_) => {
                error!(
                    "Malformed lsp_message from client. Contents: {}",
                    str::from_utf8(lsp_message_bytes)
                        .map(|str| str.to_string())
                        .unwrap_or_else(|_| format!("{:?}", lsp_message_bytes))
                );
                Err(())
            }
            Ok(None) => Ok(None),
            Ok(Some(parsed)) => Ok(Some(parsed)),
        }
    }

    async fn proxy_request(&mut self, request: Request) -> Result<(), ServerStream::Error> {
        let mut raw_message = Vec::new();
        LspMessage::Request(request.clone())
            .write(&mut raw_message)
            .expect("writing to a vec to never fail");

        let raw_message = TokioWsMessage::Text(
            raw_message
                .try_into()
                .expect("raw_message to always contain UTF-8 bytes"),
        );

        async { self.server_sink.lock().await.send(raw_message).await }
            .await
            .map_err(|err| {
                error!("Failed to proxy a request to the server. Error: {err}");
                err
            })
    }

    async fn proxy_response(&mut self, response: Response) -> Result<(), ServerStream::Error> {
        let mut raw_message = Vec::new();
        LspMessage::Response(response.clone())
            .write(&mut raw_message)
            .expect("writing to a vec to never fail");

        let raw_message = TokioWsMessage::Text(
            raw_message
                .try_into()
                .expect("raw_message to always contain UTF-8 bytes"),
        );

        async { self.server_sink.lock().await.send(raw_message).await }
            .await
            .map_err(|err| {
                error!("Failed to proxy a response to the server. Error: {err}");
                err
            })
    }

    async fn send_response_skipping_server(
        &mut self,
        response: Response,
        received_time: OffsetDateTime,
    ) -> Result<(), ClientStream::Error> {
        let mut raw_message = Vec::new();
        LspMessage::Response(response.clone())
            .write(&mut raw_message)
            .expect("writing to a vec to never fail");

        let raw_message = raw_message
            .try_into()
            .expect("raw_message to always contain UTF-8 bytes");

        let raw_message = AxumWsMessage::Text(raw_message);

        let send_future = async {
            async { self.client_sink.lock().await.send(raw_message).await }
                .await
                .map_err(|err| {
                    error!("Failed to send an interrupt response back to the client. Error: {err}");
                    err
                })
        };

        let log_future = async {
            self.session
                .log_response(
                    response,
                    MessageSource::Proxy,
                    received_time + Duration::from_millis(1),
                )
                .await
                .ok();
        };

        tokio::join!(send_future, log_future).0
    }

    async fn proxy_notification(
        &mut self,
        notification: Notification,
    ) -> Result<(), ServerStream::Error> {
        let mut raw_message = Vec::new();
        LspMessage::Notification(notification.clone())
            .write(&mut raw_message)
            .expect("writing to a vec to never fail");

        let raw_message = TokioWsMessage::Text(
            raw_message
                .try_into()
                .expect("raw_message to always contain UTF-8 bytes"),
        );

        self.server_sink.lock().await.send(raw_message).await
    }

    fn validate_message_kind_and_sender(kind: MessageKind, method: &str) {
        LspSession::validate_message_kind(kind, method);
        match LspSession::get_expected_sender(kind, method) {
            None => {
                /* assume that this branch will already be coverd by message kind validation. */
            }
            Some(ExpectedSender::Server) => {
                error!(
                    "Expected `{method}` request to be sent by the server, but received one from the client."
                );
            }
            Some(ExpectedSender::Unknown) => {
                warn!("Received unknown `{method}` message method type.");
            }
            Some(ExpectedSender::Client) | Some(ExpectedSender::Either) => {}
        }
    }
}

impl<'session, ServerStream, ClientStream, ClientError>
    ClientToServerProxy<
        'session,
        Arc<RwLock<Uninitialized>>,
        ServerStream,
        ClientStream,
        ClientError,
    >
where
    ClientStream: Stream<Item = Result<AxumWsMessage, ClientError>> + Sink<AxumWsMessage> + Unpin,
    ClientStream::Error: Display,
    ClientError: Display,
    ServerStream: Sink<TokioWsMessage> + Unpin,
    ServerStream::Error: Display,
{
    fn new(
        source: SplitStream<ClientStream>,
        server_sink: Arc<Mutex<SplitSink<ServerStream, TokioWsMessage>>>,
        client_sink: Arc<Mutex<SplitSink<ClientStream, AxumWsMessage>>>,
        shared_state: Arc<RwLock<Uninitialized>>,
        session: &'session LspSession<'session>,
    ) -> Self {
        Self {
            source,
            server_sink,
            client_sink,
            state: shared_state,
            session,
        }
    }

    async fn initialize(
        mut self,
    ) -> Result<
        ClientToServerProxy<'session, InitializedState, ServerStream, ClientStream, ClientError>,
        (),
    > {
        while let Some(client_msg) = self.source.next().await {
            let received_time = OffsetDateTime::now_utc();

            if *self.session.exited.read().await {
                return Err(());
            }

            let msg = match client_msg {
                Err(err) => {
                    error!(
                        "Encountered an error in the websocket connection. Error: {}",
                        err
                    );
                    // client disconnected
                    *self.session.exited.write().await = true;
                    return Err(());
                }
                Ok(msg) => msg,
            };

            let message = match Self::parse_message(msg) {
                Err(()) | Ok(None) => continue,
                Ok(Some(message)) => message,
            };

            match message {
                LspMessage::Request(request) => {
                    let span = self
                        .session
                        .start_request(request.clone(), MessageSource::Client, received_time)
                        .await;

                    if self.handle_request(request, received_time).instrument(span).await.is_err() {
                        break;
                    }
                }
                LspMessage::Response(response) => {
                    let (_, span) = self
                        .session
                        .start_response(response.clone(), MessageSource::Client, received_time)
                        .await;

                    if self.handle_response(response).instrument(span).await.is_err() {
                        break;
                    }
                }
                LspMessage::Notification(notification) => {
                    let span = self
                        .session
                        .start_notification(
                            notification.clone(),
                            MessageSource::Client,
                            received_time,
                        )
                        .await;

                    span.in_scope(|| {
                        Self::validate_message_kind_and_sender(
                            MessageKind::Notification,
                            notification.method.as_str(),
                        );
                    });

                    match notification.method.as_str() {
                        Initialized::METHOD => {
                            let value = span
                                .in_scope(|| serde_json::from_value(notification.params.clone()));
                            match value {
                                Ok(params) => {
                                    let new_state;
                                    {
                                        let mut write_lock =
                                            self.state.write().instrument(span.clone()).await;
                                        if write_lock.initialize_params.is_none()
                                            || write_lock.initialize_result.is_none()
                                        {
                                            span.in_scope(|| {
                                                write_lock.failed = true;
                                                error!(
                                                    "Received `{}` notification before initialize request was processed.",
                                                    Initialized::METHOD
                                                );
                                                drop(write_lock);
                                            });
                                            // just hang up
                                            *self
                                                .session
                                                .exited
                                                .write()
                                                .instrument(span.clone())
                                                .await = true;
                                            return Err(());
                                        }

                                        write_lock.initialized_params = Some(params);

                                        let read_lock = write_lock.downgrade();
                                        new_state = InitializedState {
                                            initialize_params: read_lock
                                                .initialize_params
                                                .clone()
                                                .unwrap_or_default(),
                                            initialize_result: read_lock
                                                .initialize_result
                                                .clone()
                                                .unwrap_or_default(),
                                            initialized_params: read_lock
                                                .initialized_params
                                                .clone()
                                                .expect(
                                                    "initialized params to be populated always",
                                                ),
                                        }
                                    };

                                    return Ok(ClientToServerProxy {
                                        source: self.source,
                                        server_sink: self.server_sink,
                                        client_sink: self.client_sink,
                                        state: new_state,
                                        session: self.session,
                                    });
                                }
                                Err(err) => {
                                    span.in_scope(|| {
                                        error!(
                                            "Failed to parse `{}` params. Error: {}",
                                            Initialized::METHOD,
                                            err
                                        );
                                    });

                                    self.state
                                        .write()
                                        .instrument(span.clone())
                                        .await
                                        .initialized_params = Some(InitializedParams {})
                                }
                            }
                        }
                        other => {
                            span.in_scope(|| {
                                error!(
                                    "received a `{}` notification before initialization was completed.",
                                    other
                                );
                            });
                        }
                    }

                    match self
                        .proxy_notification(notification)
                        .instrument(span.clone())
                        .await
                    {
                        Ok(()) => {}
                        Err(err) => {
                            span.in_scope(|| {
                                error!(
                                    "Failed to proxy a notification to the server. Error: {err}"
                                );
                            });
                            break;
                        }
                    }
                }
            }
        }

        *self.session.exited.write().await = true;
        Err(())
    }

    async fn handle_request(
        &mut self,
        request: Request,
        received_at: OffsetDateTime,
    ) -> Result<(), ()> {
        Self::validate_message_kind_and_sender(MessageKind::Request, request.method.as_str());

        if let Initialize::METHOD = request.method.as_str() {
            let initialize_params = match serde_json::from_value::<InitializeParams>(
                request.params.clone(),
            ) {
                Ok(initialize_params) => initialize_params,
                Err(err) => {
                    match self
                        .send_response_skipping_server(
                            Response::new_err(
                                request.id,
                                ErrorCode::InvalidParams as i32,
                                format!("{}", err),
                            ),
                            received_at,
                        )
                        .await
                    {
                        Ok(()) => return Ok(()),
                        Err(err) => {
                            error!(
                                "Failed to send an interrupt response back to the client. Error: {err}"
                            );
                            return Err(());
                        }
                    }
                }
            };

            let params_clone_for_storage = initialize_params.clone();
            {
                let mut write_lock = self.state.write().await;
                if write_lock.initialized_params.is_some() {
                    drop(write_lock);

                    match self
                        .send_response_skipping_server(
                            Response::new_err(
                                request.id,
                                ErrorCode::RequestFailed as i32,
                                "The server already received an initialize request.".to_string(),
                            ),
                            received_at,
                        )
                        .await
                    {
                        Ok(()) => return Ok(()),
                        Err(err) => {
                            error!(
                                "Failed to send an interrupt response back to the client. Error: {err}"
                            );
                            return Err(());
                        }
                    };
                }

                write_lock.initialize_params = Some(params_clone_for_storage);
            }

            match self.proxy_request(request).await {
                Ok(()) => Ok(()),
                Err(err) => {
                    error!("Failed to proxy a response to the client. Error: {err}");
                    Err(())
                }
            }
        } else {
            match self
                .send_response_skipping_server(
                    Response::new_err(
                        request.id,
                        ErrorCode::ServerNotInitialized as i32,
                        "".to_string(),
                    ),
                    received_at,
                )
                .await
            {
                Ok(()) => Ok(()),
                Err(err) => {
                    error!("Failed to send an interrupt response back to the client. Error: {err}");
                    Err(())
                }
            }
        }
    }

    async fn handle_response(&mut self, response: Response) -> Result<(), ()> {
        match self.session.get_request_for_response(&response).await {
            Err(_) | Ok(None) => {}
            Ok(Some((_, request))) => {
                Self::validate_message_kind_and_sender(
                    MessageKind::Response,
                    request.method.as_str(),
                );

                match request.method.as_str() {
                    ShowMessageRequest::METHOD => {}
                    other => {
                        error!(
                            "Received response for `{other}` request before initialization was complete."
                        );
                    }
                }

                LspSession::validate_response(request.method.as_str(), response.clone());
            }
        };

        match self.proxy_response(response).await {
            Ok(()) => Ok(()),
            Err(err) => {
                error!("Failed to proxy a response to the client. Error: {err}");
                Err(())
            }
        }
    }
}

impl<'session, ServerStream, ClientStream, ClientError>
    ClientToServerProxy<'session, InitializedState, ServerStream, ClientStream, ClientError>
where
    ClientStream: Stream<Item = Result<AxumWsMessage, ClientError>> + Sink<AxumWsMessage> + Unpin,
    ClientStream::Error: Display,
    ClientError: Display,
    ServerStream: Sink<TokioWsMessage> + Unpin,
    ServerStream::Error: Display,
{
    async fn run_proxy(
        mut self,
    ) -> Result<ClientToServerProxy<'session, Shutdown, ServerStream, ClientStream, ClientError>, ()>
    {
        while let Some(client_msg) = self.source.next().await {
            let received_time = OffsetDateTime::now_utc();

            let msg = match client_msg {
                Err(err) => {
                    error!(
                        "Encountered an error in the websocket connection. Error: {}",
                        err
                    );
                    // client disconnected
                    *self.session.exited.write().await = true;
                    return Err(());
                }
                Ok(msg) => msg,
            };

            let message = match Self::parse_message(msg) {
                Err(()) | Ok(None) => continue,
                Ok(Some(message)) => message,
            };

            match message {
                LspMessage::Request(request) => {
                    let span = self
                        .session
                        .start_request(request.clone(), MessageSource::Client, received_time)
                        .await;

                    self.handle_request(request).instrument(span).await?;
                }
                LspMessage::Response(response) => {
                    let (request_id, span) = self
                        .session
                        .start_response(response.clone(), MessageSource::Client, received_time)
                        .await;

                    self.handle_response(request_id, response)
                        .instrument(span)
                        .await?;
                }
                LspMessage::Notification(notification) => {
                    let span = self
                        .session
                        .start_notification(
                            notification.clone(),
                            MessageSource::Client,
                            received_time,
                        )
                        .await;

                    self.handle_notification(notification)
                        .instrument(span)
                        .await?;
                }
            }
        }

        *self.session.exited.write().await = true;
        Err(())
    }

    async fn handle_request(&mut self, request: Request) -> Result<(), ()> {
        Self::validate_message_kind_and_sender(MessageKind::Request, request.method.as_str());

        match LspSession::validate_request_params(request.clone()).await {
            Ok(()) => {}
            Err(response) => {
                match self
                    .send_response_skipping_server(response, OffsetDateTime::now_utc())
                    .await
                {
                    Ok(()) => {}
                    Err(err) => {
                        error!(
                            "Failed to send an interrupt response back to the client. Error: {err}"
                        );
                        *self.session.exited.write().await = true;
                        return Err(());
                    }
                };
            }
        };

        match self.proxy_request(request).await {
            Ok(()) => Ok(()),
            Err(err) => {
                error!("Failed to proxy a request to the server. Error: {err}");
                *self.session.exited.write().await = true;
                Err(())
            }
        }
    }

    async fn handle_response(
        &mut self,
        request_id: Option<i64>,
        response: Response,
    ) -> Result<(), ()> {
        if let Some(db_id) = request_id {
            match self
                .session
                .get_request_from_id(db_id, response.id.clone())
                .await
            {
                Err(_) => {}
                Ok(request) => {
                    Self::validate_message_kind_and_sender(
                        MessageKind::Response,
                        request.method.as_str(),
                    );

                    LspSession::validate_response(request.method.as_str(), response.clone());
                }
            };
        }

        match self.proxy_response(response).await {
            Ok(()) => Ok(()),
            Err(err) => {
                error!("Failed to proxy a response to the client. Error: {err}");
                *self.session.exited.write().await = true;
                return Err(());
            }
        }
    }

    async fn handle_notification(&mut self, notification: Notification) -> Result<(), ()> {
        Self::validate_message_kind_and_sender(
            MessageKind::Notification,
            notification.method.as_str(),
        );
        LspSession::validate_notification_params(notification.clone());

        match notification.method.as_str() {
            Initialized::METHOD => {
                error!(
                    "Received an `{}` notification after initialization had already been completed.",
                    Initialized::METHOD,
                );
            }
            _ => {}
        }

        match self.proxy_notification(notification).await {
            Ok(()) => Ok(()),
            Err(err) => {
                error!("Failed to proxy a response to the client. Error: {err}");
                *self.session.exited.write().await = true;
                return Err(());
            }
        }
    }
}

impl<'session, ServerStream, ClientStream, ClientError>
    ClientToServerProxy<'session, Shutdown, ServerStream, ClientStream, ClientError>
where
    ClientStream: Stream<Item = Result<AxumWsMessage, ClientError>> + Sink<AxumWsMessage> + Unpin,
    ClientStream::Error: Display,
    ClientError: Display,
    ServerStream: Sink<TokioWsMessage> + Unpin,
    ServerStream::Error: Display,
{
    async fn listen_for_exit(mut self) {
        while let Some(client_msg) = self.source.next().await {
            let received_time = OffsetDateTime::now_utc();

            if *self.session.exited.read().await {
                return;
            }

            let msg = match client_msg {
                Err(err) => {
                    error!(
                        "Encountered an error in the websocket connection. Error: {}",
                        err
                    );
                    // client disconnected
                    *self.session.exited.write().await = true;
                    return;
                }
                Ok(msg) => msg,
            };

            let message = match Self::parse_message(msg) {
                Err(()) | Ok(None) => continue,
                Ok(Some(message)) => message,
            };

            match message {
                LspMessage::Request(request) => {
                    let span = self
                        .session
                        .start_request(request.clone(), MessageSource::Server, received_time)
                        .await;

                    self.handle_request(request, received_time)
                        .instrument(span)
                        .await;
                }
                LspMessage::Response(response) => {
                    let (_, span) = self
                        .session
                        .start_response(response.clone(), MessageSource::Server, received_time)
                        .await;

                    self.handle_response(response).instrument(span).await;
                }
                LspMessage::Notification(notification) => {
                    let span = self
                        .session
                        .start_notification(
                            notification.clone(),
                            MessageSource::Server,
                            received_time,
                        )
                        .await;

                    self.handle_notification(notification)
                        .instrument(span)
                        .await;
                }
            }
        }

        *self.session.exited.write().await = true;
    }

    async fn handle_request(&mut self, request: Request, received_at: OffsetDateTime) {
        Self::validate_message_kind_and_sender(MessageKind::Request, request.method.as_str());

        match self
            .send_response_skipping_server(
                Response::new_err(
                    request.id,
                    ErrorCode::RequestFailed as i32,
                    "Server was shut down.".to_string(),
                ),
                received_at,
            )
            .await
        {
            Ok(()) => {}
            Err(err) => {
                error!("Failed to send an interrupt response back to the client. Error: {err}");
                *self.session.exited.write().await = true;
                return;
            }
        }
    }

    async fn handle_response(&mut self, response: Response) {
        match self.proxy_response(response).await {
            Ok(()) => {}
            Err(err) => {
                error!("Failed to proxy a response to the server. Error: {err}");
                *self.session.exited.write().await = true;
                return;
            }
        };
    }

    async fn handle_notification(&mut self, notification: Notification) {
        Self::validate_message_kind_and_sender(
            MessageKind::Notification,
            notification.method.as_str(),
        );

        match self.proxy_notification(notification).await {
            Ok(()) => {}
            Err(err) => {
                error!("Failed to proxy a notification to the server. Error: {err}");
                *self.session.exited.write().await = true;
                return;
            }
        }
    }
}

struct InitializedState {
    initialize_params: InitializeParams,
    initialize_result: InitializeResult,
    initialized_params: InitializedParams,
}
