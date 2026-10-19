use std::{collections::HashMap, sync::Arc};

use futures::future;
use serde_json::Value;
use sqlx::{PgPool, Row};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::message::{Conversation, Message, Notification, Request, RequestId, Response};

#[derive(Clone, Debug)]
pub(crate) struct MessageWithTimeStamp {
    pub(crate) db_id: i64,
    pub(crate) time_stamp: OffsetDateTime,
    pub(crate) message: Message,
    pub(crate) source: MessageSource,
}

pub(crate) enum ExpectedSender {
    Client,
    Server,
    Either,
    Unknown,
}

#[repr(i8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MessageSource {
    Unknown = -1,
    Client = 0,
    Server = 1,
    Proxy = 2,
}

impl From<i32> for MessageSource {
    fn from(value: i32) -> Self {
        match value {
            0 => MessageSource::Client,
            1 => MessageSource::Server,
            2 => MessageSource::Proxy,
            _ => MessageSource::Unknown,
        }
    }
}

struct RequestRecord {
    id: i64,
    request_id: RequestId,
    session_id: i64,
    method: String,
    params: Value,
    time_stamp: OffsetDateTime,
    source: i32,
}

struct OkResponseRecord {
    id: i64,
    request_id: i64,
    session_id: i64,
    result: Value,
    time_stamp: OffsetDateTime,
    source: i32,
}

struct ErrResponseRecord {
    id: i64,
    request_id: i64,
    session_id: i64,
    error_code: i32,
    error_message: String,
    error_data: Option<Value>,
    time_stamp: OffsetDateTime,
    source: i32,
}

struct NotificationRecord {
    id: i64,
    session_id: i64,
    method: String,
    params: Value,
    time_stamp: OffsetDateTime,
    source: i32,
}

pub(crate) async fn get_all_messages_for_session_in_chronological_order(
    db: &PgPool,
    session_id: i64,
) -> Result<Conversation, sqlx::Error> {
    let requests =
        sqlx::query("SELECT * FROM requests WHERE session_id = $1 ORDER BY time_stamp ASC")
            .bind(session_id)
            .map(|row: sqlx::postgres::PgRow| {
                let request_id = row.get::<String, _>("request_id");
                RequestRecord {
                    id: row.get("id"),
                    request_id: if let Ok(int) = request_id.parse::<i32>() {
                        RequestId::from(int)
                    } else {
                        RequestId::from(request_id)
                    },
                    session_id: row.get("session_id"),
                    method: row.get("method"),
                    params: row.get("params"),
                    time_stamp: row.get("time_stamp"),
                    source: row.get("source"),
                }
            })
            .fetch_all(db);

    let responses =
        sqlx::query("SELECT * FROM responses WHERE session_id = $1 ORDER BY time_stamp ASC")
            .bind(session_id)
            .map(|row: sqlx::postgres::PgRow| {
                let id = row.get("id");
                let request_id = row.get("request_id");
                let session_id = row.get("session_id");
                let time_stamp = row.get("time_stamp");
                let source = row.get("source");

                if row.get("is_error") {
                    Err(ErrResponseRecord {
                        id,
                        request_id,
                        session_id,
                        error_code: row.get("error_code"),
                        error_message: row.get("error_message"),
                        error_data: row.get("error_data"),
                        time_stamp: time_stamp,
                        source,
                    })
                } else {
                    Ok(OkResponseRecord {
                        id,
                        request_id,
                        session_id,
                        result: row.get("result"),
                        time_stamp,
                        source,
                    })
                }
            })
            .fetch_all(db);

    let notifications =
        sqlx::query("SELECT * FROM notifications WHERE session_id = $1 ORDER BY time_stamp ASC")
            .bind(session_id)
            .map(|row: sqlx::postgres::PgRow| NotificationRecord {
                id: row.get("id"),
                session_id: row.get("session_id"),
                method: row.get("method"),
                params: row.get("params"),
                time_stamp: row.get("time_stamp"),
                source: row.get("source"),
            })
            .fetch_all(db);

    let (requests_result, responses_result, notifications_result) =
        future::join3(requests, responses, notifications).await;

    let requests = requests_result?;
    let responses = responses_result?;
    let notifications = notifications_result?;

    let requests_ref = &requests;
    let mut all_messages = responses
        .into_iter()
        .map(|response_record| {
            let response_id = requests_ref
                .iter()
                .find(|request| {
                    request.id
                        == *match &response_record {
                            Ok(response) => &response.request_id,
                            Err(response) => &response.request_id,
                        }
                })
                .map(|match_| match_.request_id.clone())
                .unwrap_or_else(|| RequestId::from(Uuid::new_v4().to_string()));

            let response_db_id;
            let response_time_stamp;
            let response;
            let source;
            match response_record {
                Ok(ok_response) => {
                    response_db_id = ok_response.id;
                    response_time_stamp = ok_response.time_stamp;
                    response = Response::new_ok(response_id, ok_response.result);
                    source = ok_response.source;
                }
                Err(err_response) => {
                    response_db_id = err_response.id;
                    response_time_stamp = err_response.time_stamp;
                    response = Response::new_err(
                        response_id,
                        err_response.error_code,
                        err_response.error_message,
                    );
                    source = err_response.source;
                }
            };

            MessageWithTimeStamp {
                db_id: response_db_id,
                time_stamp: response_time_stamp,
                message: Message::Response(response),
                source: MessageSource::from(source),
            }
        })
        .collect::<Vec<_>>();

    all_messages.extend(
        requests
            .into_iter()
            .map(|request_record| MessageWithTimeStamp {
                db_id: request_record.id,
                time_stamp: request_record.time_stamp,
                message: Message::Request(Request::new(
                    RequestId::from(request_record.request_id),
                    request_record.method,
                    request_record.params,
                )),
                source: MessageSource::from(request_record.source),
            }),
    );

    all_messages.extend(
        notifications
            .into_iter()
            .map(|notification| MessageWithTimeStamp {
                db_id: notification.id,
                time_stamp: notification.time_stamp,
                message: Message::Notification(Notification::new(
                    notification.method,
                    notification.params,
                )),
                source: MessageSource::from(notification.source),
            }),
    );

    all_messages.sort_by(|message_with_time_stamp1, message_with_time_stamp2| {
        message_with_time_stamp1
            .time_stamp
            .cmp(&message_with_time_stamp2.time_stamp)
    });

    Ok(all_messages.into())
}

pub(crate) struct SessionErrors {
    pub(crate) session_id: i64,
    pub(crate) session_errs: Vec<ErrorRow>,
    pub(crate) request_errs: HashMap<i64, Vec<ErrorRow>>,
    pub(crate) response_errs: HashMap<i64, Vec<ErrorRow>>,
    pub(crate) notification_errs: HashMap<i64, Vec<ErrorRow>>,
}

pub(crate) struct ErrorRow {
    pub(crate) id: i64,
    pub(crate) time_stamp: OffsetDateTime,
    pub(crate) message: Arc<str>,
}

impl<'a> From<&'a RawErrorRow> for ErrorRow {
    fn from(value: &'a RawErrorRow) -> Self {
        Self {
            id: value.id,
            time_stamp: value.time_stamp.clone(),
            message: value.message.clone(),
        }
    }
}

struct RawErrorRow {
    id: i64,
    time_stamp: OffsetDateTime,
    request_id: Option<i64>,
    response_id: Option<i64>,
    notification_id: Option<i64>,
    message: Arc<str>,
}

pub(crate) async fn get_all_errs_for_session(
    db: &PgPool,
    session_id: i64,
) -> Result<SessionErrors, sqlx::Error> {
    let errors = sqlx::query("SELECT id, time_stamp, request_id, response_id, notification_id, message FROM logs WHERE session_id = $1")
        .bind(session_id)
        .map(|row: sqlx::postgres::PgRow| {
            RawErrorRow {
                id: row.get::<i64, _>("id"),
                time_stamp: row.get::<OffsetDateTime, _>("time_stamp"),
                request_id: row.get::<Option<i64>, _>("request_id"),
                response_id: row.get::<Option<i64>, _>("response_id"),
                notification_id: row.get::<Option<i64>, _>("notification_id"),
                message: row.get::<String, _>("message").into(),
            }
        })
        .fetch_all(db)
        .await?;

    let mut session_errs = Vec::new();
    let mut request_errs = HashMap::new();
    let mut response_errs = HashMap::new();
    let mut notification_errs = HashMap::new();

    for err in errors {
        let mut is_attached_to_message = false;
        if let Some(request_id) = err.request_id {
            is_attached_to_message = true;
            let errs = request_errs.entry(request_id).or_insert_with(|| Vec::new());
            errs.push(ErrorRow::from(&err));
        }

        if let Some(response_id) = err.response_id {
            is_attached_to_message = true;
            let errs = response_errs
                .entry(response_id)
                .or_insert_with(|| Vec::new());
            errs.push(ErrorRow::from(&err));
        }

        if let Some(notification_id) = err.notification_id {
            is_attached_to_message = true;
            let errs = notification_errs
                .entry(notification_id)
                .or_insert_with(|| Vec::new());
            errs.push(ErrorRow::from(&err));
        }

        if !is_attached_to_message {
            session_errs.push(ErrorRow::from(&err));
        }
    }

    Ok(SessionErrors {
        session_id,
        session_errs,
        request_errs,
        response_errs,
        notification_errs,
    })
}
