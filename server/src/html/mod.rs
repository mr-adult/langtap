use std::collections::HashSet;

use axum::{
    extract::{Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::Html,
};
use futures::FutureExt;
use serde::Deserialize;
use sqlx::Row;
use time::OffsetDateTime;

use crate::{
    AppState,
    html::chat_view::append_chat_html_to,
    message::{Conversation, MessageClassification, classify},
    session::SessionErrors,
};

mod chat_view;
pub(crate) mod session_search;

#[derive(Deserialize)]
pub(crate) struct GetSessionParams {
    session_id: i64,
    life_cycle: Option<bool>,
    document_synchronization: Option<bool>,
    notebook_synchronization: Option<bool>,
    workspace_synchronization: Option<bool>,
    workspace: Option<bool>,
    telemetry: Option<bool>,
    declaration: Option<bool>,
    definition: Option<bool>,
    type_definition: Option<bool>,
    implementation: Option<bool>,
    references: Option<bool>,
    call_hierarchy: Option<bool>,
    type_hierarchy: Option<bool>,
    document_highlight: Option<bool>,
    document_link: Option<bool>,
    hover: Option<bool>,
    code_lens: Option<bool>,
    folding_range: Option<bool>,
    selection: Option<bool>,
    symbol: Option<bool>,
    semantic_tokens: Option<bool>,
    inlay_hint: Option<bool>,
    inline_value: Option<bool>,
    moniker: Option<bool>,
    completion: Option<bool>,
    diagnostic: Option<bool>,
    signature_help: Option<bool>,
    code_action: Option<bool>,
    document_color: Option<bool>,
    formatting: Option<bool>,
    rename: Option<bool>,
    linked_editing_range: Option<bool>,
    execute_command: Option<bool>,
    uncategorized: Option<bool>,
}

impl GetSessionParams {
    fn build_message_classification_allow_list(&self) -> HashSet<Option<MessageClassification>> {
        let request = self;

        let show_all = request.life_cycle.is_none()
            && request.document_synchronization.is_none()
            && request.notebook_synchronization.is_none()
            && request.workspace_synchronization.is_none()
            && request.workspace.is_none()
            && request.telemetry.is_none()
            && request.declaration.is_none()
            && request.definition.is_none()
            && request.type_definition.is_none()
            && request.implementation.is_none()
            && request.references.is_none()
            && request.call_hierarchy.is_none()
            && request.type_hierarchy.is_none()
            && request.document_highlight.is_none()
            && request.document_link.is_none()
            && request.hover.is_none()
            && request.code_lens.is_none()
            && request.folding_range.is_none()
            && request.selection.is_none()
            && request.symbol.is_none()
            && request.semantic_tokens.is_none()
            && request.inlay_hint.is_none()
            && request.inline_value.is_none()
            && request.moniker.is_none()
            && request.completion.is_none()
            && request.diagnostic.is_none()
            && request.signature_help.is_none()
            && request.code_action.is_none()
            && request.document_color.is_none()
            && request.formatting.is_none()
            && request.rename.is_none()
            && request.linked_editing_range.is_none()
            && request.execute_command.is_none()
            && request.uncategorized.is_none();

        let mut msg_types_to_include = HashSet::new();
        if show_all || matches!(request.life_cycle, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::Lifecycle));
        }
        if show_all || matches!(request.document_synchronization, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::TextDocumentSynchronization));
        }
        if show_all || matches!(request.notebook_synchronization, Some(true)) {
            msg_types_to_include
                .insert(Some(MessageClassification::NotebookDocumentSynchronization));
        }
        if show_all || matches!(request.workspace_synchronization, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::WorkspaceSynchronization));
        }
        if show_all || matches!(request.workspace, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::Workspace));
        }
        if show_all || matches!(request.telemetry, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::Telemetry));
        }
        if show_all || matches!(request.declaration, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::Declaration));
        }
        if show_all || matches!(request.definition, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::Definition));
        }
        if show_all || matches!(request.type_definition, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::TypeDefinition));
        }
        if show_all || matches!(request.implementation, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::Implementation));
        }
        if show_all || matches!(request.references, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::References));
        }
        if show_all || matches!(request.call_hierarchy, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::CallHierarchy));
        }
        if show_all || matches!(request.type_hierarchy, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::TypeHierarchy));
        }
        if show_all || matches!(request.document_highlight, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::DocumentHighlight));
        }
        if show_all || matches!(request.document_link, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::DocumentLink));
        }
        if show_all || matches!(request.hover, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::Hover));
        }
        if show_all || matches!(request.code_lens, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::CodeLens));
        }
        if show_all || matches!(request.folding_range, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::FoldingRange));
        }
        if show_all || matches!(request.selection, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::Selection));
        }
        if show_all || matches!(request.symbol, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::Symbol));
        }
        if show_all || matches!(request.semantic_tokens, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::SemanticTokens));
        }
        if show_all || matches!(request.inlay_hint, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::InlayHint));
        }
        if show_all || matches!(request.inline_value, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::InlineValue));
        }
        if show_all || matches!(request.moniker, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::Moniker));
        }
        if show_all || matches!(request.completion, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::Completion));
        }
        if show_all || matches!(request.diagnostic, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::Diagnostic));
        }
        if show_all || matches!(request.signature_help, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::SignatureHelp));
        }
        if show_all || matches!(request.code_action, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::CodeAction));
        }
        if show_all || matches!(request.document_color, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::DocumentColor));
        }
        if show_all || matches!(request.formatting, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::Formatting));
        }
        if show_all || matches!(request.rename, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::Rename));
        }
        if show_all || matches!(request.linked_editing_range, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::LinkedEditingRange));
        }
        if show_all || matches!(request.execute_command, Some(true)) {
            msg_types_to_include.insert(Some(MessageClassification::ExecuteCommand));
        }
        if show_all || matches!(request.uncategorized, Some(true)) {
            msg_types_to_include.insert(None);
        }
        msg_types_to_include
    }
}

struct Session {
    id: i64,
    end_time_stamp: Option<OffsetDateTime>,
}

pub(crate) async fn get_session(
    State(state): State<AppState>,
    Query(request): Query<GetSessionParams>,
) -> Result<(StatusCode, HeaderMap, Html<String>), StatusCode> {
    let (session, conversation, protocol_errs) = tokio::join! {
        sqlx::query("SELECT id, end_time_stamp FROM sessions WHERE id = $1 LIMIT 1;")
            .bind(request.session_id)
            .fetch_one(&state.db)
            .map(|row| {
                row.map(|row| Session {
                    id: row.get("id"),
                    end_time_stamp: row.get("end_time_stamp"),
                })
            }),
        crate::session::get_all_messages_for_session_in_chronological_order(
            &state.db,
            request.session_id
        ),
        crate::session::get_all_errs_for_session(&state.db, request.session_id)
    };

    let map_sql_err_to_http = |err| match err {
        sqlx::Error::RowNotFound => StatusCode::NOT_FOUND,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };

    let session = session.map_err(map_sql_err_to_http)?;
    let conversation = conversation.map_err(map_sql_err_to_http)?;
    let protocol_errs = protocol_errs.map_err(map_sql_err_to_http)?;

    let mut html = String::new();
    html.push_str("<!DOCTYPE=html>");
    html.push_str("<html>");

    html.push_str("<head>");
    html.push_str("<meta charset=\"UTF-8\"/>");
    html.push_str("<title>LSP Analyzer</title>");
    html.push_str("</head>");

    html.push_str("<body>");
    {
        html.push_str("<style>");
        html.push_str(include_str!("../css/general.css"));
        html.push_str(include_str!("../css/branding.css"));
        html.push_str(include_str!("../css/chat.css"));
        html.push_str("</style>");

        html.push_str(include_str!("./branding.html"));
        html.push_str(r#"<div id="page_layout">"#);
        {
            html.push_str(&generate_filtering_form(&request, &conversation));
            html.push_str("<div>");
            {
                html.push_str(&generate_session_errors_card(&protocol_errs));

                let allow_list = request.build_message_classification_allow_list();
                append_chat_html_to(
                    &mut html,
                    session.id,
                    &conversation,
                    &allow_list,
                    &protocol_errs,
                );
            }
            html.push_str("</div>");
        }
        html.push_str("</div>");
    }
    html.push_str("</body>");
    html.push_str("</html>");

    // if the session has been ended, it won't change. Set long-lived caching headers
    let mut headers = HeaderMap::new();
    if session.end_time_stamp.is_some() {
        headers.insert("immutable", HeaderValue::from_str("true").unwrap());
    } else {
        headers.insert("no-store", HeaderValue::from_str("true").unwrap());
    }

    Ok((StatusCode::OK, headers, Html(html)))
}

fn generate_filtering_form(request: &GetSessionParams, conversation: &Conversation) -> String {
    let message_types_in_conversation = conversation
        .messages()
        .into_iter()
        .map(|message_with_time_stamp| classify(&message_with_time_stamp.message, conversation))
        .collect::<HashSet<_>>();

    let allow_list = request.build_message_classification_allow_list();

    let mut html = String::new();

    html.push_str("<form id=\"filter_form\" action=\"/session\" method=\"GET\">");
    html.push_str("<h2>Filter Your Results</h2>");
    html.push_str("<fieldset id=\"filter_fieldset\">");
    html.push_str("<legend>Filter Messages by Category:</legend>");
    html.push_str("<input type=\"text\" id=\"session_id\" name=\"session_id\" style=\"display: none;\" value=\"");
    html.push_str(&request.session_id.to_string());
    html.push_str("\">");

    for msg_kind in MessageClassification::all() {
        if !message_types_in_conversation.contains(&Some(*msg_kind)) {
            continue;
        }

        html.push_str("<span>");
        let msg_name = msg_kind.as_str();
        let msg_id = msg_name.replace(" ", "_");
        html.push_str("<input type=\"checkbox\" id=\"");
        html.push_str(&msg_id);
        html.push_str("\" name=\"");
        html.push_str(&msg_id);
        html.push_str("\" value=\"");
        html.push_str("true");
        html.push_str("\"");

        if allow_list.contains(&Some(*msg_kind)) {
            html.push_str(" checked");
        }

        html.push('>');

        html.push_str("<label for=\"");
        html.push_str(&msg_id);
        html.push_str("\">");
        html.push_str(msg_name);
        html.push_str("</label><br/>");
        html.push_str("</span>");
    }

    if message_types_in_conversation.contains(&None) {
        html.push_str("<span>");
        html.push_str("<input type=\"checkbox\" id=\"uncategorized\" name=\"uncategorized\"");

        if allow_list.contains(&None) {
            html.push_str(" checked");
        }
        html.push('>');

        html.push_str("<label for=\"uncategorized\">uncategorized</label><br/>");
        html.push_str("</span>");
    }

    html.push_str("</fieldset>");

    html.push_str("<button type=\"Submit\">Update Results</button>");

    html.push_str("</form>");

    html
}

fn generate_session_errors_card(protocol_errs: &SessionErrors) -> String {
    if protocol_errs.session_errs.is_empty() {
        return String::default();
    }

    let mut html = String::new();

    html.push_str(r###"<div style="border-radius:10px; padding:16px; color: white; background: var(--client-color)">"###);
    html.push_str("<details>");
    html.push_str("<summary>");
    html.push_str(r###"<h3 style="display:inline-block; color: white;">This session encountered errors</h3></summary>"###);
    html.push_str("<ul>");
    for err in protocol_errs.session_errs.iter() {
        html.push_str("<li>");
        html.push_str("<span>");
        html.push_str(&html_escape::encode_text(&err.message));
        html.push_str("</span>");
        html.push_str("</li>");
    }
    html.push_str("</ul>");
    html.push_str("</details>");
    html.push_str("</div>");
    html
}
