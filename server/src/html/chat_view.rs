use std::{collections::HashSet, iter::Enumerate, vec::IntoIter};

use crate::{message::Message, session::SessionErrors};
use serde_json::{Map, Value};

use crate::{
    message::{Conversation, MessageClassification, classify},
    session::MessageSource,
    utils::get_iso_string,
};

pub(crate) fn append_chat_html_to(
    html: &mut String,
    session_id: i64,
    conversation: &Conversation,
    allow_list: &HashSet<Option<MessageClassification>>,
    protocol_errs: &SessionErrors,
) {
    html.push_str("<div id=\"chat\">");
    {
        for message_with_time_stamp in conversation {
            if !allow_list.contains(&classify(&message_with_time_stamp.message, conversation)) {
                continue;
            }

            let source = message_with_time_stamp.source;
            let message = &message_with_time_stamp.message;

            let class_name;
            let message_wrapper_class;
            match source {
                MessageSource::Client => {
                    class_name = "client_message";
                    message_wrapper_class = "client_message_wrapper";
                }
                MessageSource::Server => {
                    class_name = "server_message";
                    message_wrapper_class = "server_message_wrapper";
                }
                MessageSource::Proxy | MessageSource::Unknown => {
                    class_name = "message";
                    message_wrapper_class = "message_wrapper";
                }
            };

            html.push_str("<div class=\"");
            html.push_str(message_wrapper_class);
            html.push_str("\">");
            {
                html.push_str("<div class=\"");
                {
                    html.push_str(class_name);
                    html.push_str("\">");
                    html.push_str("<details class=\"message_summary");
                    match source {
                        MessageSource::Client => html.push_str(" client"),
                        MessageSource::Server => html.push_str(" server"),
                        MessageSource::Proxy | MessageSource::Unknown => {}
                    }
                    html.push_str("\">");
                    {
                        let errs;
                        html.push_str("<summary>");
                        {
                            match &message {
                                Message::Request(req) => {
                                    if let Some(request_errs) = protocol_errs
                                        .request_errs
                                        .get(&message_with_time_stamp.db_id)
                                    {
                                        errs = Some(request_errs);
                                        html.push_str("⚠️ ");
                                    } else {
                                        errs = None;
                                    }

                                    html.push_str("Request: ");
                                    html.push_str(&req.method);
                                }
                                Message::Response(resp) => {
                                    if let Some(response_errs) = protocol_errs
                                        .response_errs
                                        .get(&message_with_time_stamp.db_id)
                                    {
                                        errs = Some(response_errs);
                                        html.push_str("⚠️ ");
                                    } else {
                                        errs = None;
                                    }

                                    html.push_str("Response: ");
                                    let method = conversation
                                        .requests()
                                        .get(&resp.id)
                                        .map(|request| &request.method);
                                    if let Some(method) = method {
                                        html.push_str(method);
                                    } else {
                                        html.push_str("Unknown Response");
                                    }
                                }
                                Message::Notification(not) => {
                                    if let Some(notification_errs) = protocol_errs
                                        .notification_errs
                                        .get(&message_with_time_stamp.db_id)
                                    {
                                        errs = Some(notification_errs);
                                        html.push_str("⚠️ ");
                                    } else {
                                        errs = None;
                                    }

                                    html.push_str("Notification: ");
                                    html.push_str(&not.method);
                                }
                            }
                        }
                        html.push_str("</summary>");

                        let empty = Vec::with_capacity(0);
                        let errs = errs.unwrap_or(&empty);
                        if !errs.is_empty() {
                            html.push_str("Errors:");
                            html.push_str("<ul>");
                            for err in errs {
                                html.push_str("<li>");
                                html.push_str(&html_escape::encode_text(&err.message));
                                html.push_str("</li>");
                            }
                            html.push_str("</ul>");
                        }

                        append_json_html_to(html, serde_json::to_value(message.clone()).unwrap());
                    }
                    html.push_str("</details>");
                }
                html.push_str("</div>");

                html.push_str(
                    r#"<div style="display: flex; flex-direction: column; row-gap: 4px;"#,
                );
                match source {
                    MessageSource::Client => html.push_str("margin-left: 50px; text-align: left;"),
                    MessageSource::Server => {
                        html.push_str("margin-right: 50px; text-align: right;")
                    }
                    MessageSource::Unknown | MessageSource::Proxy => {}
                }
                html.push_str(r#"">"#);
                {
                    html.push_str(
                        r#"<div style="display: flex; flex-direction: row; align-items: center; column-gap: 4px; "#,
                    );
                    match source {
                        MessageSource::Client => html.push_str(r#"justify-content: flex-start;"#),
                        MessageSource::Server => html.push_str(r#"justify-content: flex-end;"#),
                        MessageSource::Unknown | MessageSource::Proxy => {}
                    }
                    html.push_str(r#"">"#);
                    {
                        html.push_str(get_replay_svg());
                        html.push_str(r#"<a style="color: var(--link-color);""#);
                        html.push_str(r#"href="/replay?session_id="#);
                        html.push_str(&session_id.to_string());
                        html.push_str(r#"&termination_message="#);
                        html.push_str(&message_with_time_stamp.db_id.to_string());
                        html.push_str(r#"&message_kind="#);
                        html.push_str(
                            (message_with_time_stamp.message.kind() as u8)
                                .to_string()
                                .as_str(),
                        );
                        html.push_str(r#"" title="replay the session up to this message">"#);
                        html.push_str("replay");
                        html.push_str("</a>");
                    }
                    html.push_str("</div>");

                    html.push_str("<span class=\"timestamp\">");
                    html.push_str(&get_iso_string(&message_with_time_stamp.time_stamp));
                    html.push_str("</span>");
                }
                html.push_str("</div>");
            }
            html.push_str("</div>");
        }
    }
    html.push_str("</div>");
}

fn get_replay_svg() -> &'static str {
    r###"<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 48 48"><g xmlns="http://www.w3.org/2000/svg" transform="scale(2)"><path d="M12 20.75a7.26 7.26 0 0 1-7.25-7.25.75.75 0 0 1 1.5 0A5.75 5.75 0 1 0 12 7.75H9.5a.75.75 0 0 1 0-1.5H12a7.25 7.25 0 0 1 0 14.5" style="fill: rgb(255, 255, 255);"></path><path d="M12 10.75a.74.74 0 0 1-.53-.22l-3-3a.75.75 0 0 1 0-1.06l3-3a.75.75 0 1 1 1.06 1.06L10.06 7l2.47 2.47a.75.75 0 0 1 0 1.06.74.74 0 0 1-.53.22" style="fill: rgb(255, 255, 255);"></path></g></svg>"###
}

fn append_json_html_to(html: &mut String, value: Value) {
    html.push_str("<div class=\"json-view\">");
    append_json_html_to_internal(html, value);
    html.push_str("</div>");
}

enum TokenKind {
    ObjectStart,
    ObjectEnd,
    ArrayStart,
    ArrayEnd,
    Colon,
    Comma,
    String,
    Number,
    True,
    False,
    Null,
}

enum ValueIter {
    Array((bool, Enumerate<IntoIter<Value>>)),
    Object(
        (
            bool,
            Enumerate<<Map<String, Value> as IntoIterator>::IntoIter>,
        ),
    ),
}

fn append_json_html_to_internal(html: &mut String, value: Value) {
    let mut stack = Vec::new();

    let null_token = r#"<span class="json-null">null</span>"#;
    let true_token = r#"<span class="json-true">true</span>"#;
    let false_token = r#"<span class="json-false">false</span>"#;
    let number_open = r#"<span class="json-number">"#;
    let number_close = r#"</span>"#;

    match value {
        Value::Null => html.push_str(null_token),
        Value::Bool(value) => {
            if value {
                html.push_str(true_token);
            } else {
                html.push_str(false_token);
            }
        }
        Value::Number(number) => {
            html.push_str(number_open);
            html.push_str(&number.to_string());
            html.push_str(number_close);
        }
        Value::String(str) => {
            append_escaped_string(html, &str);
        }
        Value::Array(values) => {
            stack.push(ValueIter::Array((false, values.into_iter().enumerate())));
        }
        Value::Object(map) => {
            stack.push(ValueIter::Object((false, map.into_iter().enumerate())));
        }
    }

    let mut previous = None;
    let mut indent = 0;
    let indent_str = "&nbsp;&nbsp;";
    while let Some(top) = stack.pop() {
        match top {
            ValueIter::Array((any_values, mut values)) => {
                if !any_values {
                    if let Some(TokenKind::ArrayStart | TokenKind::ObjectStart) = previous {
                        html.push_str("<br/>");
                        for _ in 0..indent {
                            html.push_str(indent_str);
                        }
                    }

                    html.push_str(r#"<span class="json-bracket-"#);
                    html.push_str(&(indent % 3).to_string());
                    html.push_str(r#"">[</span>"#);
                    previous = Some(TokenKind::ArrayStart);
                    indent += 1;
                }

                let mut done_with_values = true;
                while let Some((i, value)) = values.next() {
                    if i != 0 {
                        html.push(',');
                        html.push_str("<br/>");
                        for _ in 0..indent {
                            html.push_str(indent_str);
                        }
                        previous = Some(TokenKind::Comma);
                    }

                    match value {
                        Value::Null => {
                            if let Some(TokenKind::ArrayStart | TokenKind::ObjectStart) = previous {
                                html.push_str("<br/>");
                                for _ in 0..indent {
                                    html.push_str(indent_str);
                                }
                            }

                            html.push_str(null_token);
                            previous = Some(TokenKind::Null);
                        }
                        Value::Bool(value) => {
                            if let Some(TokenKind::ArrayStart | TokenKind::ObjectStart) = previous {
                                html.push_str("<br/>");
                                for _ in 0..indent {
                                    html.push_str(indent_str);
                                }
                            }

                            if value {
                                html.push_str(true_token);
                                previous = Some(TokenKind::True);
                            } else {
                                html.push_str(false_token);
                                previous = Some(TokenKind::False);
                            }
                        }
                        Value::Number(number) => {
                            if let Some(TokenKind::ArrayStart | TokenKind::ObjectStart) = previous {
                                html.push_str("<br/>");
                                for _ in 0..indent {
                                    html.push_str(indent_str);
                                }
                            }

                            html.push_str(number_open);
                            html.push_str(&number.to_string());
                            html.push_str(number_close);
                            previous = Some(TokenKind::Number);
                        }
                        Value::String(str) => {
                            if let Some(TokenKind::ArrayStart | TokenKind::ObjectStart) = previous {
                                html.push_str("<br/>");
                                for _ in 0..indent {
                                    html.push_str(indent_str);
                                }
                            }

                            append_escaped_string(html, &str);
                            previous = Some(TokenKind::String);
                        }
                        Value::Array(inner_values) => {
                            stack.push(ValueIter::Array((true, values)));
                            stack.push(ValueIter::Array((
                                false,
                                inner_values.into_iter().enumerate(),
                            )));
                            done_with_values = false;
                            break;
                        }
                        Value::Object(map) => {
                            stack.push(ValueIter::Array((true, values)));
                            stack.push(ValueIter::Object((false, map.into_iter().enumerate())));
                            done_with_values = false;
                            break;
                        }
                    }
                }

                if done_with_values {
                    indent -= 1;
                    if let Some(TokenKind::ArrayStart) = previous {
                    } else {
                        html.push_str("<br/>");
                        for _ in 0..indent {
                            html.push_str(indent_str);
                        }
                    }

                    html.push_str(r#"<span class="json-bracket-"#);
                    html.push_str(&(indent % 3).to_string());
                    html.push_str(r#"">]</span>"#);
                    previous = Some(TokenKind::ArrayEnd);
                }
            }
            ValueIter::Object((any_values, mut key_values)) => {
                if !any_values {
                    if let Some(TokenKind::ArrayStart | TokenKind::ObjectStart) = previous {
                        html.push_str("<br/>");
                        for _ in 0..indent {
                            html.push_str(indent_str);
                        }
                    }

                    html.push_str(r#"<span class="json-bracket-"#);
                    html.push_str(&(indent % 3).to_string());
                    html.push_str(r#"">{</span>"#);
                    previous = Some(TokenKind::ObjectStart);
                    indent += 1;
                }

                let mut done_with_values = true;
                while let Some((i, (key, value))) = key_values.next() {
                    if i != 0 {
                        html.push(',');
                        html.push_str("<br/>");
                        for _ in 0..indent {
                            html.push_str(indent_str);
                        }
                        // previous = Some(TokenKind::Comma);
                    } else if let Some(TokenKind::ArrayStart | TokenKind::ObjectStart) = previous {
                        html.push_str("<br/>");
                        for _ in 0..indent {
                            html.push_str(indent_str);
                        }
                    }

                    html.push_str(r#"<span class="json-key">"#);
                    html.push('"');
                    html_escape::encode_text_to_string(&key, html);
                    html.push('"');
                    html.push_str(r#"</span>"#);
                    html.push_str(": ");
                    previous = Some(TokenKind::Colon);

                    match value {
                        Value::Null => {
                            html.push_str(null_token);
                            previous = Some(TokenKind::Null);
                        }
                        Value::Bool(value) => {
                            if value {
                                html.push_str(true_token);
                                previous = Some(TokenKind::True);
                            } else {
                                html.push_str(false_token);
                                previous = Some(TokenKind::False);
                            }
                        }
                        Value::Number(number) => {
                            html.push_str(number_open);
                            html.push_str(&number.to_string());
                            html.push_str(number_close);
                            previous = Some(TokenKind::Number);
                        }
                        Value::String(str) => {
                            append_escaped_string(html, &str);
                            previous = Some(TokenKind::String);
                        }
                        Value::Array(values) => {
                            stack.push(ValueIter::Object((true, key_values)));
                            stack.push(ValueIter::Array((false, values.into_iter().enumerate())));
                            done_with_values = false;
                            break;
                        }
                        Value::Object(map) => {
                            stack.push(ValueIter::Object((true, key_values)));
                            stack.push(ValueIter::Object((false, map.into_iter().enumerate())));
                            done_with_values = false;
                            break;
                        }
                    }
                }

                if done_with_values {
                    indent -= 1;
                    if let Some(TokenKind::ObjectStart) = previous {
                    } else {
                        html.push_str("<br/>");
                        for _ in 0..indent {
                            html.push_str(indent_str);
                        }
                    }

                    html.push_str(r#"<span class="json-bracket-"#);
                    html.push_str(&(indent % 3).to_string());
                    html.push_str(r#"">}</span>"#);
                    previous = Some(TokenKind::ObjectEnd);
                }
            }
        }
    }
}

fn append_escaped_string(html: &mut String, content: &str) {
    html.push_str(r#"<span class="json-string">"#);
    html.push('"');
    html_escape::encode_text_to_string(content, html);
    html.push('"');
    html.push_str(r#"</span>"#);
}

#[test]
fn test() {
    let mut map = Map::new();
    map.insert(
        "test".to_string(),
        Value::Array(vec![Value::String("test".to_string())]),
    );
    let mut html = String::new();
    let obj = Value::Object(map);
    append_json_html_to(&mut html, Value::Array(vec![obj]));
    println!("{}", html);
}
