use std::{collections::HashMap, sync::Arc};

use lsp_types::{
    notification::{
        Cancel, DidChangeConfiguration, DidChangeNotebookDocument, DidChangeTextDocument,
        DidChangeWatchedFiles, DidChangeWorkspaceFolders, DidCloseNotebookDocument,
        DidCloseTextDocument, DidCreateFiles, DidDeleteFiles, DidOpenNotebookDocument,
        DidOpenTextDocument, DidRenameFiles, DidSaveNotebookDocument, DidSaveTextDocument, Exit,
        Initialized, LogMessage, LogTrace, Notification as LspNotification, Progress,
        PublishDiagnostics, SetTrace, ShowMessage, TelemetryEvent, WillSaveTextDocument,
        WorkDoneProgressCancel,
    },
    request::{
        ApplyWorkspaceEdit, CallHierarchyIncomingCalls, CallHierarchyOutgoingCalls,
        CallHierarchyPrepare, CodeActionRequest, CodeActionResolveRequest, CodeLensRefresh,
        CodeLensRequest, CodeLensResolve, ColorPresentationRequest, Completion, DocumentColor,
        DocumentDiagnosticRequest, DocumentHighlightRequest, DocumentLinkRequest,
        DocumentLinkResolve, DocumentSymbolRequest, ExecuteCommand, FoldingRangeRequest,
        Formatting, GotoDeclaration, GotoDefinition, GotoImplementation, GotoTypeDefinition,
        HoverRequest, Initialize, InlayHintRefreshRequest, InlayHintRequest,
        InlayHintResolveRequest, InlineValueRefreshRequest, InlineValueRequest, LinkedEditingRange,
        MonikerRequest, OnTypeFormatting, PrepareRenameRequest, RangeFormatting, References,
        RegisterCapability, Rename, Request as LspRequest, ResolveCompletionItem,
        SelectionRangeRequest, SemanticTokensFullDeltaRequest, SemanticTokensFullRequest,
        SemanticTokensRangeRequest, SemanticTokensRefresh, ShowDocument, ShowMessageRequest,
        Shutdown, SignatureHelpRequest, TypeHierarchyPrepare, TypeHierarchySubtypes,
        TypeHierarchySupertypes, UnregisterCapability, WillCreateFiles, WillDeleteFiles,
        WillRenameFiles, WillSaveWaitUntil, WorkDoneProgressCreate, WorkspaceConfiguration,
        WorkspaceDiagnosticRefresh, WorkspaceDiagnosticRequest, WorkspaceFoldersRequest,
        WorkspaceSymbolRequest, WorkspaceSymbolResolve,
    },
};
use serde_json::Value;
use sqlx::{PgPool, Row};
use time::OffsetDateTime;
use tokio::sync::RwLock;
use tracing::{Instrument, Span, error, info_span};

use crate::{
    message::{ErrorCode, MessageKind, Notification, Request, RequestId, Response},
    session::{ExpectedSender, MessageSource},
};

#[derive(PartialEq, Eq)]
enum ExpectedMessageKind {
    RequestOrResponse,
    Notification,
}

impl ExpectedMessageKind {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            ExpectedMessageKind::RequestOrResponse => "request or response",
            ExpectedMessageKind::Notification => "notification",
        }
    }
}

impl From<MessageKind> for ExpectedMessageKind {
    fn from(value: MessageKind) -> Self {
        match value {
            MessageKind::Notification => ExpectedMessageKind::Notification,
            MessageKind::Request | MessageKind::Response => ExpectedMessageKind::RequestOrResponse,
        }
    }
}

#[derive(Clone)]
pub(super) struct LspSession<'db> {
    pub(super) session_id: i64,
    pub(super) exited: Arc<RwLock<bool>>,
    pub(super) unresponded_requests: Arc<RwLock<HashMap<RequestId, i64>>>,
    db: &'db PgPool,
}

impl<'db> LspSession<'db> {
    pub(crate) fn new(session_id: i64, db: &'db PgPool) -> Self {
        Self {
            session_id,
            exited: Arc::new(RwLock::new(false)),
            unresponded_requests: Arc::new(RwLock::new(HashMap::new())),
            db,
        }
    }

    pub(super) fn validate_message_kind(kind: MessageKind, method: &str) {
        let actual = ExpectedMessageKind::from(kind);
        let expected = match method {
            Cancel::METHOD | Progress::METHOD => Some(ExpectedMessageKind::Notification),
            Initialize::METHOD => Some(ExpectedMessageKind::RequestOrResponse),
            Initialized::METHOD => Some(ExpectedMessageKind::Notification),
            RegisterCapability::METHOD | UnregisterCapability::METHOD => {
                Some(ExpectedMessageKind::RequestOrResponse)
            }
            SetTrace::METHOD | LogTrace::METHOD => Some(ExpectedMessageKind::Notification),
            Shutdown::METHOD => Some(ExpectedMessageKind::RequestOrResponse),
            Exit::METHOD
            | DidOpenTextDocument::METHOD
            | DidChangeTextDocument::METHOD
            | WillSaveTextDocument::METHOD => Some(ExpectedMessageKind::Notification),
            WillSaveWaitUntil::METHOD => Some(ExpectedMessageKind::RequestOrResponse),
            DidSaveTextDocument::METHOD
            | DidCloseTextDocument::METHOD
            | DidOpenNotebookDocument::METHOD
            | DidChangeNotebookDocument::METHOD
            | DidSaveNotebookDocument::METHOD
            | DidCloseNotebookDocument::METHOD => Some(ExpectedMessageKind::Notification),
            GotoDeclaration::METHOD
            | GotoDefinition::METHOD
            | GotoTypeDefinition::METHOD
            | GotoImplementation::METHOD
            | References::METHOD
            | CallHierarchyPrepare::METHOD
            | CallHierarchyIncomingCalls::METHOD
            | CallHierarchyOutgoingCalls::METHOD
            | TypeHierarchyPrepare::METHOD
            | TypeHierarchySupertypes::METHOD
            | TypeHierarchySubtypes::METHOD
            | DocumentHighlightRequest::METHOD
            | DocumentLinkRequest::METHOD
            | DocumentLinkResolve::METHOD
            | HoverRequest::METHOD
            | CodeLensRequest::METHOD
            | CodeLensResolve::METHOD
            | CodeLensRefresh::METHOD
            | FoldingRangeRequest::METHOD
            | SelectionRangeRequest::METHOD
            | DocumentSymbolRequest::METHOD
            | SemanticTokensFullRequest::METHOD
            | SemanticTokensFullDeltaRequest::METHOD
            | SemanticTokensRangeRequest::METHOD
            | SemanticTokensRefresh::METHOD
            | InlayHintRequest::METHOD
            | InlayHintResolveRequest::METHOD
            | InlayHintRefreshRequest::METHOD
            | InlineValueRequest::METHOD
            | InlineValueRefreshRequest::METHOD
            | MonikerRequest::METHOD
            | Completion::METHOD
            | ResolveCompletionItem::METHOD => Some(ExpectedMessageKind::RequestOrResponse),
            PublishDiagnostics::METHOD => Some(ExpectedMessageKind::Notification),
            DocumentDiagnosticRequest::METHOD
            | WorkspaceDiagnosticRequest::METHOD
            | WorkspaceDiagnosticRefresh::METHOD
            | SignatureHelpRequest::METHOD
            | CodeActionRequest::METHOD
            | CodeActionResolveRequest::METHOD
            | DocumentColor::METHOD
            | ColorPresentationRequest::METHOD
            | Formatting::METHOD
            | RangeFormatting::METHOD
            | OnTypeFormatting::METHOD
            | Rename::METHOD
            | PrepareRenameRequest::METHOD
            | LinkedEditingRange::METHOD
            | WorkspaceSymbolRequest::METHOD
            | WorkspaceSymbolResolve::METHOD
            | WorkspaceConfiguration::METHOD => Some(ExpectedMessageKind::RequestOrResponse),
            DidChangeConfiguration::METHOD => Some(ExpectedMessageKind::Notification),
            WorkspaceFoldersRequest::METHOD => Some(ExpectedMessageKind::RequestOrResponse),
            DidChangeWorkspaceFolders::METHOD => Some(ExpectedMessageKind::Notification),
            WillCreateFiles::METHOD => Some(ExpectedMessageKind::RequestOrResponse),
            DidCreateFiles::METHOD => Some(ExpectedMessageKind::Notification),
            WillRenameFiles::METHOD => Some(ExpectedMessageKind::RequestOrResponse),
            DidRenameFiles::METHOD => Some(ExpectedMessageKind::Notification),
            WillDeleteFiles::METHOD => Some(ExpectedMessageKind::RequestOrResponse),
            DidDeleteFiles::METHOD => Some(ExpectedMessageKind::Notification),
            DidChangeWatchedFiles::METHOD => Some(ExpectedMessageKind::Notification),
            ExecuteCommand::METHOD => Some(ExpectedMessageKind::RequestOrResponse),
            ApplyWorkspaceEdit::METHOD => Some(ExpectedMessageKind::RequestOrResponse),
            ShowMessage::METHOD => Some(ExpectedMessageKind::Notification),
            ShowMessageRequest::METHOD | ShowDocument::METHOD => {
                Some(ExpectedMessageKind::RequestOrResponse)
            }
            LogMessage::METHOD => Some(ExpectedMessageKind::Notification),
            WorkDoneProgressCreate::METHOD => Some(ExpectedMessageKind::RequestOrResponse),
            WorkDoneProgressCancel::METHOD => Some(ExpectedMessageKind::Notification),
            TelemetryEvent::METHOD => Some(ExpectedMessageKind::Notification),
            _ => None,
        };

        if let Some(expected) = expected {
            if expected != actual {
                error!("Expected `{method}` to be a {}.", expected.as_str());
            }
        }
    }

    pub(super) fn get_expected_sender(kind: MessageKind, method: &str) -> Option<ExpectedSender> {
        match method {
            Cancel::METHOD => Some(ExpectedSender::Either),
            Progress::METHOD => Some(ExpectedSender::Either),
            Initialize::METHOD => match kind {
                MessageKind::Request => Some(ExpectedSender::Client),
                MessageKind::Response => Some(ExpectedSender::Server),
                MessageKind::Notification => None,
            },
            Initialized::METHOD => Some(ExpectedSender::Client),
            RegisterCapability::METHOD | UnregisterCapability::METHOD => match kind {
                MessageKind::Request => Some(ExpectedSender::Server),
                MessageKind::Response => Some(ExpectedSender::Client),
                MessageKind::Notification => None,
            },
            SetTrace::METHOD => Some(ExpectedSender::Client),
            LogTrace::METHOD => Some(ExpectedSender::Server),
            Shutdown::METHOD
            | Exit::METHOD
            | DidOpenTextDocument::METHOD
            | DidChangeTextDocument::METHOD
            | WillSaveTextDocument::METHOD => Some(ExpectedSender::Client),
            WillSaveWaitUntil::METHOD => match kind {
                MessageKind::Request => Some(ExpectedSender::Client),
                MessageKind::Response => Some(ExpectedSender::Server),
                MessageKind::Notification => None,
            },
            DidSaveTextDocument::METHOD
            | DidCloseTextDocument::METHOD
            | DidOpenNotebookDocument::METHOD
            | DidChangeNotebookDocument::METHOD
            | DidSaveNotebookDocument::METHOD
            | DidCloseNotebookDocument::METHOD => Some(ExpectedSender::Client),
            GotoDeclaration::METHOD
            | GotoDefinition::METHOD
            | GotoTypeDefinition::METHOD
            | GotoImplementation::METHOD
            | References::METHOD
            | CallHierarchyPrepare::METHOD
            | CallHierarchyIncomingCalls::METHOD
            | CallHierarchyOutgoingCalls::METHOD
            | TypeHierarchyPrepare::METHOD
            | TypeHierarchySupertypes::METHOD
            | TypeHierarchySubtypes::METHOD
            | DocumentHighlightRequest::METHOD
            | DocumentLinkRequest::METHOD
            | DocumentLinkResolve::METHOD
            | HoverRequest::METHOD
            | CodeLensRequest::METHOD
            | CodeLensResolve::METHOD => match kind {
                MessageKind::Request => Some(ExpectedSender::Client),
                MessageKind::Response => Some(ExpectedSender::Server),
                MessageKind::Notification => None,
            },
            CodeLensRefresh::METHOD => match kind {
                MessageKind::Request => Some(ExpectedSender::Server),
                MessageKind::Response => Some(ExpectedSender::Client),
                MessageKind::Notification => None,
            },
            FoldingRangeRequest::METHOD
            | SelectionRangeRequest::METHOD
            | DocumentSymbolRequest::METHOD
            | SemanticTokensFullRequest::METHOD
            | SemanticTokensFullDeltaRequest::METHOD
            | SemanticTokensRangeRequest::METHOD => match kind {
                MessageKind::Request => Some(ExpectedSender::Client),
                MessageKind::Response => Some(ExpectedSender::Server),
                MessageKind::Notification => None,
            },
            SemanticTokensRefresh::METHOD => match kind {
                MessageKind::Request => Some(ExpectedSender::Server),
                MessageKind::Response => Some(ExpectedSender::Client),
                MessageKind::Notification => None,
            },
            InlayHintRequest::METHOD | InlayHintResolveRequest::METHOD => match kind {
                MessageKind::Request => Some(ExpectedSender::Client),
                MessageKind::Response => Some(ExpectedSender::Server),
                MessageKind::Notification => None,
            },
            InlayHintRefreshRequest::METHOD => match kind {
                MessageKind::Request => Some(ExpectedSender::Server),
                MessageKind::Response => Some(ExpectedSender::Client),
                MessageKind::Notification => None,
            },
            InlineValueRequest::METHOD => match kind {
                MessageKind::Request => Some(ExpectedSender::Client),
                MessageKind::Response => Some(ExpectedSender::Server),
                MessageKind::Notification => None,
            },
            InlineValueRefreshRequest::METHOD => match kind {
                MessageKind::Request => Some(ExpectedSender::Server),
                MessageKind::Response => Some(ExpectedSender::Client),
                MessageKind::Notification => None,
            },
            MonikerRequest::METHOD | Completion::METHOD | ResolveCompletionItem::METHOD => {
                match kind {
                    MessageKind::Request => Some(ExpectedSender::Client),
                    MessageKind::Response => Some(ExpectedSender::Server),
                    MessageKind::Notification => None,
                }
            }
            PublishDiagnostics::METHOD => match kind {
                MessageKind::Request => Some(ExpectedSender::Server),
                MessageKind::Response => Some(ExpectedSender::Client),
                MessageKind::Notification => None,
            },
            DocumentDiagnosticRequest::METHOD | WorkspaceDiagnosticRequest::METHOD => match kind {
                MessageKind::Request => Some(ExpectedSender::Client),
                MessageKind::Response => Some(ExpectedSender::Server),
                MessageKind::Notification => None,
            },
            WorkspaceDiagnosticRefresh::METHOD => match kind {
                MessageKind::Request => Some(ExpectedSender::Server),
                MessageKind::Response => Some(ExpectedSender::Client),
                MessageKind::Notification => None,
            },
            SignatureHelpRequest::METHOD
            | CodeActionRequest::METHOD
            | CodeActionResolveRequest::METHOD
            | DocumentColor::METHOD
            | ColorPresentationRequest::METHOD
            | Formatting::METHOD
            | RangeFormatting::METHOD
            | OnTypeFormatting::METHOD
            | Rename::METHOD
            | PrepareRenameRequest::METHOD
            | LinkedEditingRange::METHOD
            | WorkspaceSymbolRequest::METHOD
            | WorkspaceSymbolResolve::METHOD => match kind {
                MessageKind::Request => Some(ExpectedSender::Client),
                MessageKind::Response => Some(ExpectedSender::Server),
                MessageKind::Notification => None,
            },
            WorkspaceConfiguration::METHOD => match kind {
                MessageKind::Request => Some(ExpectedSender::Server),
                MessageKind::Response => Some(ExpectedSender::Client),
                MessageKind::Notification => None,
            },
            DidChangeConfiguration::METHOD => Some(ExpectedSender::Client),
            WorkspaceFoldersRequest::METHOD => match kind {
                MessageKind::Request => Some(ExpectedSender::Server),
                MessageKind::Response => Some(ExpectedSender::Client),
                MessageKind::Notification => None,
            },
            DidChangeWorkspaceFolders::METHOD => Some(ExpectedSender::Client),
            WillCreateFiles::METHOD => match kind {
                MessageKind::Request => Some(ExpectedSender::Client),
                MessageKind::Response => Some(ExpectedSender::Server),
                MessageKind::Notification => None,
            },
            DidCreateFiles::METHOD => Some(ExpectedSender::Client),
            WillRenameFiles::METHOD => match kind {
                MessageKind::Request => Some(ExpectedSender::Client),
                MessageKind::Response => Some(ExpectedSender::Server),
                MessageKind::Notification => None,
            },
            DidRenameFiles::METHOD => Some(ExpectedSender::Client),
            WillDeleteFiles::METHOD => match kind {
                MessageKind::Request => Some(ExpectedSender::Client),
                MessageKind::Response => Some(ExpectedSender::Server),
                MessageKind::Notification => None,
            },
            DidDeleteFiles::METHOD | DidChangeWatchedFiles::METHOD => Some(ExpectedSender::Client),
            ExecuteCommand::METHOD => match kind {
                MessageKind::Request => Some(ExpectedSender::Client),
                MessageKind::Response => Some(ExpectedSender::Server),
                MessageKind::Notification => None,
            },
            ApplyWorkspaceEdit::METHOD => match kind {
                MessageKind::Request => Some(ExpectedSender::Server),
                MessageKind::Response => Some(ExpectedSender::Client),
                MessageKind::Notification => None,
            },
            ShowMessage::METHOD => Some(ExpectedSender::Server),
            ShowMessageRequest::METHOD | ShowDocument::METHOD => match kind {
                MessageKind::Request => Some(ExpectedSender::Server),
                MessageKind::Response => Some(ExpectedSender::Client),
                MessageKind::Notification => None,
            },
            LogMessage::METHOD => Some(ExpectedSender::Server),
            WorkDoneProgressCreate::METHOD => match kind {
                MessageKind::Request => Some(ExpectedSender::Server),
                MessageKind::Response => Some(ExpectedSender::Client),
                MessageKind::Notification => None,
            },
            WorkDoneProgressCancel::METHOD => Some(ExpectedSender::Client),
            TelemetryEvent::METHOD => Some(ExpectedSender::Server),
            _ => Some(ExpectedSender::Unknown),
        }
    }

    pub(super) async fn start_request(
        &self,
        request: Request,
        source: MessageSource,
        received_time: OffsetDateTime,
    ) -> Span {
        let request_id = request.id.clone();

        match self.log_request(request, source, received_time).await {
            Ok(db_request_id) => {
                let request_span = info_span!("request_id", request_id = db_request_id);

                if let Some(previous) = {
                    self.unresponded_requests
                        .write()
                        .instrument(request_span.clone())
                        .await
                        .insert(request_id, db_request_id)
                } {
                    error!("Received a duplicate request with ID: `{previous}`");
                }

                request_span
            }
            Err(err) => {
                error!("Failed to log request. Error: {}", err);
                info_span!("request_id")
            }
        }
    }

    async fn log_request(
        &self,
        request: Request,
        source: MessageSource,
        received_time: OffsetDateTime,
    ) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar(
                "INSERT INTO requests (request_id, session_id, method, params, time_stamp, source) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id;"
            )
            .bind(request.id.to_string())
            .bind(self.session_id)
            .bind(request.method)
            .bind(request.params)
            .bind(received_time)
            .bind(source as i32)
            .fetch_one(self.db)
            .await
    }

    pub(super) async fn validate_request_params(request: Request) -> Result<(), Response> {
        match request.method.as_str() {
            Initialize::METHOD => Self::validate_request_params_inner::<Initialize>(request),
            RegisterCapability::METHOD => {
                Self::validate_request_params_inner::<RegisterCapability>(request)
            }
            UnregisterCapability::METHOD => {
                Self::validate_request_params_inner::<UnregisterCapability>(request)
            }
            Shutdown::METHOD => Self::validate_request_params_inner::<Shutdown>(request),
            WillSaveWaitUntil::METHOD => {
                Self::validate_request_params_inner::<WillSaveWaitUntil>(request)
            }
            GotoDeclaration::METHOD => {
                Self::validate_request_params_inner::<GotoDeclaration>(request)
            }
            GotoDefinition::METHOD => {
                Self::validate_request_params_inner::<GotoDefinition>(request)
            }
            GotoTypeDefinition::METHOD => {
                Self::validate_request_params_inner::<GotoTypeDefinition>(request)
            }
            GotoImplementation::METHOD => {
                Self::validate_request_params_inner::<GotoImplementation>(request)
            }
            References::METHOD => Self::validate_request_params_inner::<References>(request),
            CallHierarchyPrepare::METHOD => {
                Self::validate_request_params_inner::<CallHierarchyPrepare>(request)
            }
            CallHierarchyIncomingCalls::METHOD => {
                Self::validate_request_params_inner::<CallHierarchyIncomingCalls>(request)
            }
            CallHierarchyOutgoingCalls::METHOD => {
                Self::validate_request_params_inner::<CallHierarchyOutgoingCalls>(request)
            }
            TypeHierarchyPrepare::METHOD => {
                Self::validate_request_params_inner::<TypeHierarchyPrepare>(request)
            }
            TypeHierarchySupertypes::METHOD => {
                Self::validate_request_params_inner::<TypeHierarchySupertypes>(request)
            }
            TypeHierarchySubtypes::METHOD => {
                Self::validate_request_params_inner::<TypeHierarchySubtypes>(request)
            }
            DocumentHighlightRequest::METHOD => {
                Self::validate_request_params_inner::<DocumentHighlightRequest>(request)
            }
            DocumentLinkRequest::METHOD => {
                Self::validate_request_params_inner::<DocumentLinkRequest>(request)
            }
            DocumentLinkResolve::METHOD => {
                Self::validate_request_params_inner::<DocumentLinkResolve>(request)
            }
            HoverRequest::METHOD => Self::validate_request_params_inner::<HoverRequest>(request),
            CodeLensRequest::METHOD => {
                Self::validate_request_params_inner::<CodeLensRequest>(request)
            }
            CodeLensResolve::METHOD => {
                Self::validate_request_params_inner::<CodeLensResolve>(request)
            }
            CodeLensRefresh::METHOD => {
                Self::validate_request_params_inner::<CodeLensRefresh>(request)
            }
            FoldingRangeRequest::METHOD => {
                Self::validate_request_params_inner::<FoldingRangeRequest>(request)
            }
            SelectionRangeRequest::METHOD => {
                Self::validate_request_params_inner::<SelectionRangeRequest>(request)
            }
            DocumentSymbolRequest::METHOD => {
                Self::validate_request_params_inner::<DocumentSymbolRequest>(request)
            }
            SemanticTokensFullRequest::METHOD => {
                Self::validate_request_params_inner::<SemanticTokensFullRequest>(request)
            }
            SemanticTokensFullDeltaRequest::METHOD => {
                Self::validate_request_params_inner::<SemanticTokensFullDeltaRequest>(request)
            }
            SemanticTokensRangeRequest::METHOD => {
                Self::validate_request_params_inner::<SemanticTokensRangeRequest>(request)
            }
            SemanticTokensRefresh::METHOD => {
                Self::validate_request_params_inner::<SemanticTokensRefresh>(request)
            }
            InlayHintRequest::METHOD => {
                Self::validate_request_params_inner::<InlayHintRequest>(request)
            }
            InlayHintResolveRequest::METHOD => {
                Self::validate_request_params_inner::<InlayHintResolveRequest>(request)
            }
            InlayHintRefreshRequest::METHOD => {
                Self::validate_request_params_inner::<InlayHintRefreshRequest>(request)
            }
            InlineValueRequest::METHOD => {
                Self::validate_request_params_inner::<InlineValueRequest>(request)
            }
            InlineValueRefreshRequest::METHOD => {
                Self::validate_request_params_inner::<InlineValueRefreshRequest>(request)
            }
            MonikerRequest::METHOD => {
                Self::validate_request_params_inner::<MonikerRequest>(request)
            }
            Completion::METHOD => Self::validate_request_params_inner::<Completion>(request),
            ResolveCompletionItem::METHOD => {
                Self::validate_request_params_inner::<ResolveCompletionItem>(request)
            }
            DocumentDiagnosticRequest::METHOD => {
                Self::validate_request_params_inner::<DocumentDiagnosticRequest>(request)
            }
            WorkspaceDiagnosticRequest::METHOD => {
                Self::validate_request_params_inner::<WorkspaceDiagnosticRequest>(request)
            }
            WorkspaceDiagnosticRefresh::METHOD => {
                Self::validate_request_params_inner::<WorkspaceDiagnosticRefresh>(request)
            }
            SignatureHelpRequest::METHOD => {
                Self::validate_request_params_inner::<SignatureHelpRequest>(request)
            }
            CodeActionRequest::METHOD => {
                Self::validate_request_params_inner::<CodeActionRequest>(request)
            }
            CodeActionResolveRequest::METHOD => {
                Self::validate_request_params_inner::<CodeActionResolveRequest>(request)
            }
            DocumentColor::METHOD => Self::validate_request_params_inner::<DocumentColor>(request),
            ColorPresentationRequest::METHOD => {
                Self::validate_request_params_inner::<ColorPresentationRequest>(request)
            }
            Formatting::METHOD => Self::validate_request_params_inner::<Formatting>(request),
            RangeFormatting::METHOD => {
                Self::validate_request_params_inner::<RangeFormatting>(request)
            }
            OnTypeFormatting::METHOD => {
                Self::validate_request_params_inner::<OnTypeFormatting>(request)
            }
            Rename::METHOD => Self::validate_request_params_inner::<Rename>(request),
            PrepareRenameRequest::METHOD => {
                Self::validate_request_params_inner::<PrepareRenameRequest>(request)
            }
            LinkedEditingRange::METHOD => {
                Self::validate_request_params_inner::<LinkedEditingRange>(request)
            }
            WorkspaceSymbolRequest::METHOD => {
                Self::validate_request_params_inner::<WorkspaceSymbolRequest>(request)
            }
            WorkspaceSymbolResolve::METHOD => {
                Self::validate_request_params_inner::<WorkspaceSymbolResolve>(request)
            }
            WorkspaceConfiguration::METHOD => {
                Self::validate_request_params_inner::<WorkspaceConfiguration>(request)
            }
            WorkspaceFoldersRequest::METHOD => {
                Self::validate_request_params_inner::<WorkspaceFoldersRequest>(request)
            }
            WillCreateFiles::METHOD => {
                Self::validate_request_params_inner::<WillCreateFiles>(request)
            }
            WillRenameFiles::METHOD => {
                Self::validate_request_params_inner::<WillRenameFiles>(request)
            }
            WillDeleteFiles::METHOD => {
                Self::validate_request_params_inner::<WillDeleteFiles>(request)
            }
            ExecuteCommand::METHOD => {
                Self::validate_request_params_inner::<ExecuteCommand>(request)
            }
            ApplyWorkspaceEdit::METHOD => {
                Self::validate_request_params_inner::<ApplyWorkspaceEdit>(request)
            }
            ShowMessageRequest::METHOD => {
                Self::validate_request_params_inner::<ShowMessageRequest>(request)
            }
            ShowDocument::METHOD => Self::validate_request_params_inner::<ShowDocument>(request),
            WorkDoneProgressCreate::METHOD => {
                Self::validate_request_params_inner::<WorkDoneProgressCreate>(request)
            }
            _ => Ok(()),
        }
    }

    fn validate_request_params_inner<RequestType>(request: Request) -> Result<(), Response>
    where
        RequestType: LspRequest,
    {
        match serde_json::from_value::<RequestType::Params>(request.params) {
            Ok(_) => Ok(()),
            Err(err) => Err(Response::new_err(
                request.id,
                ErrorCode::InvalidParams as i32,
                format!("{}", err),
            )),
        }
    }

    pub(super) async fn start_response(
        &self,
        response: Response,
        source: MessageSource,
        received_time: OffsetDateTime,
    ) -> (Option<i64>, Span) {
        let request_response_db_id = self
            .log_response(response.clone(), source, received_time)
            .await;

        match request_response_db_id {
            Ok(response_id) => (
                Some(response_id),
                info_span!("response_id", response_id = response_id),
            ),
            Err(err) => {
                error!("Failed to log response `{}`. Error: {}", response.id, err);
                (None, info_span!("response_id"))
            }
        }
    }

    pub(super) async fn log_response(
        &self,
        response: Response,
        source: MessageSource,
        received_time: OffsetDateTime,
    ) -> Result<i64, sqlx::Error> {
        let db_id = self.get_request_id_for_response(&response).await?;

        let is_err;
        let error_code;
        let error_message;
        let error_data;
        let result;
        if let Some(err) = response.error {
            is_err = true;
            error_code = Some(err.code);
            error_message = Some(err.message);
            error_data = err.data;
            result = None;
        } else if let Some(res) = &response.result {
            is_err = false;
            error_code = None;
            error_message = None;
            error_data = None;
            result = Some(res);
        } else {
            is_err = false;
            error_code = None;
            error_message = None;
            error_data = None;
            result = None;
        }

        sqlx::query_scalar::<_, i64>(
            "INSERT INTO responses (request_id, session_id, is_error, result, error_code, error_message, error_data, source, time_stamp) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING id;"
            )
            .bind(db_id)
            .bind(self.session_id)
            .bind(is_err)
            .bind(result)
            .bind(error_code)
            .bind(error_message)
            .bind(error_data)
            .bind(source as i32)
            .bind(received_time)
            .fetch_one(self.db)
            .await
    }

    async fn get_request_id_for_response(
        &self,
        response: &Response,
    ) -> Result<Option<i64>, sqlx::Error> {
        if let Some(matching_request) =
            { self.unresponded_requests.write().await.remove(&response.id) }
        {
            Ok(Some(matching_request))
        } else {
            match sqlx::query(
                "SELECT id FROM requests WHERE session_id = $1 AND request_id = $2 LIMIT 1;",
            )
            .bind(self.session_id)
            .bind(response.id.to_string())
            .fetch_one(self.db)
            .await
            {
                Ok(row) => return Ok(Some(row.get("id"))),
                Err(err) => {
                    error!(
                        "Did not find a matching request for response `{}`. Error: {}",
                        response.id, err
                    );
                }
            }

            Ok(None)
        }
    }

    pub(super) async fn get_request_for_response(
        &self,
        response: &Response,
    ) -> Result<Option<(i64, Request)>, sqlx::Error> {
        let db_request_id = self.get_request_id_for_response(response).await?;

        if let Some(db_request_id) = db_request_id {
            self.get_request_from_id(db_request_id, response.id.clone())
                .await
                .map(|request| Some((db_request_id, request)))
        } else {
            Ok(None)
        }
    }

    pub(super) async fn get_request_from_id(
        &self,
        db_request_id: i64,
        request_id: RequestId,
    ) -> Result<Request, sqlx::Error> {
        sqlx::query("SELECT method, params FROM requests WHERE id = $1 LIMIT 1;")
            .bind(db_request_id)
            .fetch_one(self.db)
            .await
            .map(|row| Request::new(request_id, row.get("method"), row.get::<Value, _>("params")))
            .map_err(|err| {
                error!(
                    "Did not find a matching request for database response with ID `{}`. Error: {}",
                    db_request_id, err
                );
                err
            })
    }

    pub(super) fn validate_response(method: &str, response: Response) {
        match method {
            Initialize::METHOD => {
                Self::validate_response_result_inner::<Initialize>(method, response);
            }
            RegisterCapability::METHOD => {
                Self::validate_response_result_inner::<RegisterCapability>(method, response);
            }
            UnregisterCapability::METHOD => {
                Self::validate_response_result_inner::<UnregisterCapability>(method, response);
            }
            Shutdown::METHOD => {
                Self::validate_response_result_inner::<Shutdown>(method, response);
            }
            WillSaveWaitUntil::METHOD => {
                Self::validate_response_result_inner::<WillSaveWaitUntil>(method, response)
            }
            GotoDeclaration::METHOD => {
                Self::validate_response_result_inner::<GotoDeclaration>(method, response);
            }
            GotoDefinition::METHOD => {
                Self::validate_response_result_inner::<GotoDefinition>(method, response);
            }
            GotoTypeDefinition::METHOD => {
                Self::validate_response_result_inner::<GotoTypeDefinition>(method, response);
            }
            GotoImplementation::METHOD => {
                Self::validate_response_result_inner::<GotoImplementation>(method, response);
            }
            References::METHOD => {
                Self::validate_response_result_inner::<References>(method, response);
            }
            CallHierarchyPrepare::METHOD => {
                Self::validate_response_result_inner::<CallHierarchyPrepare>(method, response);
            }
            CallHierarchyIncomingCalls::METHOD => {
                Self::validate_response_result_inner::<CallHierarchyIncomingCalls>(
                    method, response,
                );
            }
            CallHierarchyOutgoingCalls::METHOD => {
                Self::validate_response_result_inner::<CallHierarchyOutgoingCalls>(
                    method, response,
                );
            }
            TypeHierarchyPrepare::METHOD => {
                Self::validate_response_result_inner::<TypeHierarchyPrepare>(method, response)
            }
            TypeHierarchySupertypes::METHOD => {
                Self::validate_response_result_inner::<TypeHierarchySupertypes>(method, response)
            }
            TypeHierarchySubtypes::METHOD => {
                Self::validate_response_result_inner::<TypeHierarchySubtypes>(method, response)
            }
            DocumentHighlightRequest::METHOD => {
                Self::validate_response_result_inner::<DocumentHighlightRequest>(method, response)
            }
            DocumentLinkRequest::METHOD => {
                Self::validate_response_result_inner::<DocumentLinkRequest>(method, response)
            }
            DocumentLinkResolve::METHOD => {
                Self::validate_response_result_inner::<DocumentLinkResolve>(method, response)
            }
            HoverRequest::METHOD => {
                Self::validate_response_result_inner::<HoverRequest>(method, response);
            }
            CodeLensRequest::METHOD => {
                Self::validate_response_result_inner::<CodeLensRequest>(method, response)
            }
            CodeLensResolve::METHOD => {
                Self::validate_response_result_inner::<CodeLensResolve>(method, response)
            }
            CodeLensRefresh::METHOD => {
                Self::validate_response_result_inner::<CodeLensRefresh>(method, response)
            }
            FoldingRangeRequest::METHOD => {
                Self::validate_response_result_inner::<FoldingRangeRequest>(method, response)
            }
            SelectionRangeRequest::METHOD => {
                Self::validate_response_result_inner::<SelectionRangeRequest>(method, response)
            }
            DocumentSymbolRequest::METHOD => {
                Self::validate_response_result_inner::<DocumentSymbolRequest>(method, response)
            }
            SemanticTokensFullRequest::METHOD => {
                Self::validate_response_result_inner::<SemanticTokensFullRequest>(method, response);
            }
            SemanticTokensFullDeltaRequest::METHOD => Self::validate_response_result_inner::<
                SemanticTokensFullDeltaRequest,
            >(method, response),
            SemanticTokensRangeRequest::METHOD => {
                Self::validate_response_result_inner::<SemanticTokensRangeRequest>(method, response)
            }
            SemanticTokensRefresh::METHOD => {
                Self::validate_response_result_inner::<SemanticTokensRefresh>(method, response)
            }
            InlayHintRequest::METHOD => {
                Self::validate_response_result_inner::<InlayHintRequest>(method, response)
            }
            InlayHintResolveRequest::METHOD => {
                Self::validate_response_result_inner::<InlayHintResolveRequest>(method, response)
            }
            InlayHintRefreshRequest::METHOD => {
                Self::validate_response_result_inner::<InlayHintRefreshRequest>(method, response)
            }
            InlineValueRequest::METHOD => {
                Self::validate_response_result_inner::<InlineValueRequest>(method, response)
            }
            InlineValueRefreshRequest::METHOD => {
                Self::validate_response_result_inner::<InlineValueRefreshRequest>(method, response)
            }
            MonikerRequest::METHOD => {
                Self::validate_response_result_inner::<MonikerRequest>(method, response)
            }
            Completion::METHOD => {
                Self::validate_response_result_inner::<Completion>(method, response)
            }
            ResolveCompletionItem::METHOD => {
                Self::validate_response_result_inner::<ResolveCompletionItem>(method, response)
            }
            DocumentDiagnosticRequest::METHOD => {
                Self::validate_response_result_inner::<DocumentDiagnosticRequest>(method, response);
            }
            WorkspaceDiagnosticRequest::METHOD => {
                Self::validate_response_result_inner::<WorkspaceDiagnosticRequest>(method, response)
            }
            WorkspaceDiagnosticRefresh::METHOD => {
                Self::validate_response_result_inner::<WorkspaceDiagnosticRefresh>(method, response)
            }
            SignatureHelpRequest::METHOD => {
                Self::validate_response_result_inner::<SignatureHelpRequest>(method, response)
            }
            CodeActionRequest::METHOD => {
                Self::validate_response_result_inner::<CodeActionRequest>(method, response)
            }
            CodeActionResolveRequest::METHOD => {
                Self::validate_response_result_inner::<CodeActionResolveRequest>(method, response)
            }
            DocumentColor::METHOD => {
                Self::validate_response_result_inner::<DocumentColor>(method, response)
            }
            ColorPresentationRequest::METHOD => {
                Self::validate_response_result_inner::<ColorPresentationRequest>(method, response)
            }
            Formatting::METHOD => {
                Self::validate_response_result_inner::<Formatting>(method, response)
            }
            RangeFormatting::METHOD => {
                Self::validate_response_result_inner::<RangeFormatting>(method, response)
            }
            OnTypeFormatting::METHOD => {
                Self::validate_response_result_inner::<OnTypeFormatting>(method, response)
            }
            Rename::METHOD => Self::validate_response_result_inner::<Rename>(method, response),
            PrepareRenameRequest::METHOD => {
                Self::validate_response_result_inner::<PrepareRenameRequest>(method, response)
            }
            LinkedEditingRange::METHOD => {
                Self::validate_response_result_inner::<LinkedEditingRange>(method, response)
            }
            WorkspaceSymbolRequest::METHOD => {
                Self::validate_response_result_inner::<WorkspaceSymbolRequest>(method, response)
            }
            WorkspaceSymbolResolve::METHOD => {
                Self::validate_response_result_inner::<WorkspaceSymbolResolve>(method, response)
            }
            WorkspaceConfiguration::METHOD => {
                Self::validate_response_result_inner::<WorkspaceConfiguration>(method, response)
            }
            WorkspaceFoldersRequest::METHOD => {
                Self::validate_response_result_inner::<WorkspaceFoldersRequest>(method, response)
            }
            WillCreateFiles::METHOD => {
                Self::validate_response_result_inner::<WillCreateFiles>(method, response)
            }
            WillRenameFiles::METHOD => {
                Self::validate_response_result_inner::<WillRenameFiles>(method, response)
            }
            WillDeleteFiles::METHOD => {
                Self::validate_response_result_inner::<WillCreateFiles>(method, response)
            }
            ExecuteCommand::METHOD => {
                Self::validate_response_result_inner::<ExecuteCommand>(method, response)
            }
            ApplyWorkspaceEdit::METHOD => {
                Self::validate_response_result_inner::<ApplyWorkspaceEdit>(method, response)
            }
            ShowMessageRequest::METHOD => {
                Self::validate_response_result_inner::<ShowMessageRequest>(method, response);
            }
            ShowDocument::METHOD => {
                Self::validate_response_result_inner::<ShowDocument>(method, response)
            }
            WorkDoneProgressCreate::METHOD => {
                Self::validate_response_result_inner::<WorkDoneProgressCreate>(method, response)
            }
            _ => {}
        }
    }

    fn validate_response_result_inner<RequestType>(method: &str, response: Response)
    where
        RequestType: LspRequest,
    {
        if response.error.is_some() && response.result.is_some() {
            error!("Response contained both an error and result, which is forbidden.");
        } else if let Some(error) = response.error {
            if let Err(()) = ErrorCode::try_from(error.code) {
                error!("Received unrecognized error code `{}`.", error.code);
            }
        } else if let Some(result) = response.result {
            match serde_json::from_value::<RequestType::Result>(result) {
                Ok(_) => {}
                Err(err) => {
                    error!("Received invalid result for a `{method}` request. Error: {err}")
                }
            }
        } else {
            error!("Response contained neither an error nor a result. One of these is required.");
        }
    }

    pub(super) async fn start_notification(
        &self,
        notification: Notification,
        source: MessageSource,
        received_time: OffsetDateTime,
    ) -> Span {
        match self
            .log_notification(notification.clone(), source, received_time)
            .await
        {
            Ok(id) => info_span!("notification_id", notification_id = id),
            Err(err) => {
                error!("Failed to log a notification to the database. Error: {err}");
                info_span!("notification_id")
            }
        }
    }

    async fn log_notification(
        &self,
        notification: Notification,
        source: MessageSource,
        received_time: OffsetDateTime,
    ) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar::<_, i64>(
            "INSERT INTO notifications (session_id, method, params, time_stamp, source) VALUES ($1, $2, $3, $4, $5) RETURNING id;")
            .bind(self.session_id)
            .bind(notification.method)
            .bind(notification.params)
            .bind(received_time)
            .bind(source as i32)
            .fetch_one(self.db)
            .await
    }

    pub(super) fn validate_notification_params(notification: Notification) {
        match notification.method.as_str() {
            Cancel::METHOD => {
                Self::validate_notification_params_inner::<Cancel>(notification);
            }
            Progress::METHOD => {
                Self::validate_notification_params_inner::<Progress>(notification);
            }
            Initialized::METHOD => {
                Self::validate_notification_params_inner::<Initialized>(notification);
            }
            SetTrace::METHOD => {
                Self::validate_notification_params_inner::<SetTrace>(notification);
            }
            LogTrace::METHOD => {
                Self::validate_notification_params_inner::<LogTrace>(notification);
            }
            Exit::METHOD => {
                Self::validate_notification_params_inner::<Exit>(notification);
            }
            DidOpenTextDocument::METHOD => {
                Self::validate_notification_params_inner::<DidOpenTextDocument>(notification);
            }
            DidChangeTextDocument::METHOD => {
                Self::validate_notification_params_inner::<DidChangeTextDocument>(notification);
            }
            WillSaveTextDocument::METHOD => {
                Self::validate_notification_params_inner::<WillSaveTextDocument>(notification);
            }
            DidSaveTextDocument::METHOD => {
                Self::validate_notification_params_inner::<DidSaveTextDocument>(notification);
            }
            DidCloseTextDocument::METHOD => {
                Self::validate_notification_params_inner::<DidCloseTextDocument>(notification);
            }
            DidOpenNotebookDocument::METHOD => {
                Self::validate_notification_params_inner::<DidOpenNotebookDocument>(notification)
            }
            DidChangeNotebookDocument::METHOD => {
                Self::validate_notification_params_inner::<DidChangeNotebookDocument>(notification)
            }
            DidSaveNotebookDocument::METHOD => {
                Self::validate_notification_params_inner::<DidSaveNotebookDocument>(notification)
            }
            DidCloseNotebookDocument::METHOD => {
                Self::validate_notification_params_inner::<DidCloseNotebookDocument>(notification)
            }
            PublishDiagnostics::METHOD => {
                Self::validate_notification_params_inner::<PublishDiagnostics>(notification)
            }
            DidChangeConfiguration::METHOD => {
                Self::validate_notification_params_inner::<DidChangeConfiguration>(notification)
            }
            DidChangeWorkspaceFolders::METHOD => {
                Self::validate_notification_params_inner::<DidChangeWorkspaceFolders>(notification)
            }
            DidCreateFiles::METHOD => {
                Self::validate_notification_params_inner::<DidCreateFiles>(notification)
            }
            DidRenameFiles::METHOD => {
                Self::validate_notification_params_inner::<DidRenameFiles>(notification)
            }
            DidDeleteFiles::METHOD => {
                Self::validate_notification_params_inner::<DidDeleteFiles>(notification)
            }
            DidChangeWatchedFiles::METHOD => {
                Self::validate_notification_params_inner::<DidChangeWatchedFiles>(notification)
            }
            ShowMessage::METHOD => {
                Self::validate_notification_params_inner::<ShowMessage>(notification)
            }
            LogMessage::METHOD => {
                Self::validate_notification_params_inner::<LogMessage>(notification)
            }
            WorkDoneProgressCancel::METHOD => {
                Self::validate_notification_params_inner::<WorkDoneProgressCancel>(notification)
            }
            TelemetryEvent::METHOD => {
                Self::validate_notification_params_inner::<TelemetryEvent>(notification)
            }
            _ => {}
        }
    }

    fn validate_notification_params_inner<NotificationType>(notification: Notification)
    where
        NotificationType: LspNotification,
    {
        match serde_json::from_value::<NotificationType::Params>(notification.params) {
            Ok(_) => {}
            Err(err) => error!(
                "Received invalid params for a `{}` notification. Error: {err}",
                notification.method.as_str()
            ),
        }
    }
}
