use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Html,
};
use serde::Deserialize;
use sqlx::{Row, prelude::FromRow};
use time::OffsetDateTime;

use crate::{AppState, utils::get_iso_string};

#[derive(FromRow)]
struct Session {
    id: i64,
    start_time_stamp: OffsetDateTime,
    end_time_stamp: Option<OffsetDateTime>,
}

#[repr(u8)]
#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
enum SortColumn {
    StartTime = 0,
    EndTime = 1,
}

impl TryFrom<usize> for SortColumn {
    type Error = ();
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(SortColumn::StartTime),
            1 => Ok(SortColumn::EndTime),
            _ => Err(()),
        }
    }
}

#[derive(Deserialize)]
pub(crate) struct PagedSessionRequest {
    page: Option<usize>,
    sort_by: Option<usize>,
    sort_asc: Option<bool>,
}

pub(crate) async fn get_sessions(
    State(state): State<AppState>,
    Query(request): Query<PagedSessionRequest>,
) -> Result<Html<String>, StatusCode> {
    let mut order_bys = Vec::new();
    if let Some(sort_by) = request.sort_by {
        let column = match SortColumn::try_from(sort_by).map_err(|_| StatusCode::BAD_REQUEST)? {
            SortColumn::StartTime => "start_time_stamp",
            SortColumn::EndTime => "end_time_stamp",
        };

        if request.sort_asc.unwrap_or(false) {
            order_bys.push((column, "ASC"));
        } else {
            order_bys.push((column, "DESC"));
        }
    }
    
    order_bys.push(("id", "DESC"));

    let mut order_by = String::new();
    order_by.push_str("ORDER BY ");
    for (i, order) in order_bys.iter().enumerate() {
        if i != 0 {
            order_by.push_str(", ");
        }

        order_by.push_str(order.0);
        order_by.push(' ');
        order_by.push_str(order.1);
    }

    let sessions = sqlx::query(&format!(
        "SELECT id, start_time_stamp, end_time_stamp FROM sessions {} LIMIT 100 OFFSET {};",
        order_by,
        request.page.unwrap_or(0) * 100
    ))
    .map(|row: sqlx::postgres::PgRow| Session {
        id: row.get("id"),
        start_time_stamp: row.get("start_time_stamp"),
        end_time_stamp: row.get("end_time_stamp"),
    })
    .fetch_all(&state.db)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut html = String::new();
    html.push_str("<!DOCTYPE=html>");
    html.push_str("<html>");

    html.push_str("<head>");
    html.push_str("<meta charset=\"UTF-8\"/>");
    html.push_str("<title>LSP Analyzer</title>");
    html.push_str("</head>");

    html.push_str("<body>");
    html.push_str("<style>");
    html.push_str(include_str!("../css/general.css"));
    html.push_str(include_str!("../css/branding.css"));
    html.push_str(include_str!("../css/sessions.css"));
    html.push_str("</style>");
    html.push_str(include_str!("./branding.html"));
    html.push_str("<div class=\"table_container\">");
    html.push_str("<table>");
    html.push_str("<thead>");
    html.push_str("<tr>");
    html.push_str("<th>ID</th>");

    html.push_str("<th><a href=\"");
    html.push_str(&build_sorted_query_string(&request, SortColumn::StartTime));
    html.push_str("\">Start Time</a></th>");

    html.push_str("<th><a href=\"");
    html.push_str(&build_sorted_query_string(&request, SortColumn::EndTime));
    html.push_str("\">End Time</a></th>");

    html.push_str("<th>Link</th>");
    html.push_str("</tr>");
    html.push_str("</thead>");

    html.push_str("<tbody>");
    for session in sessions {
        html.push_str("<tr>");

        html.push_str("<td>");
        html.push_str(&session.id.to_string());
        html.push_str("</td>");

        html.push_str("<td>");
        html.push_str(&get_iso_string(&session.start_time_stamp));
        html.push_str("</td>");

        html.push_str("<td>");
        if let Some(end_time_stamp) = &session.end_time_stamp {
            html.push_str(&get_iso_string(end_time_stamp));
        } else {
            html.push_str(r#"<span class="status_active">Active</span>"#);
        }
        html.push_str("</td>");

        html.push_str("<td>");
        html.push_str("<a href=\"/session?session_id=");
        html.push_str(&session.id.to_string());
        html.push_str("\">");
        html.push_str("/session?session_id=");
        html.push_str(&session.id.to_string());
        html.push_str("</a>");
        html.push_str("</td>");

        html.push_str("</tr>");
    }
    html.push_str("</tbody>");

    html.push_str("</table>");
    html.push_str("</div>");
    html.push_str("</body>");

    html.push_str("</html>");

    Ok(Html(html))
}

fn build_sorted_query_string(
    request: &PagedSessionRequest,
    sort_column_to_toggle: SortColumn,
) -> String {
    let mut url = "/?".to_string();
    if let Some(page) = request.page {
        url.push_str("page=");
        url.push_str(&page.to_string());

        url.push('&');
    }

    match request.sort_by {
        None => {
            url.push_str("sort_by=");
            url.push_str(&(sort_column_to_toggle as u8).to_string());
            return url;
        }
        Some(sort_by) => {
            url.push_str("sort_by=");
            url.push_str(&(sort_column_to_toggle as u8).to_string());

            url.push('&');
            url.push_str("sort_asc=");
            if sort_by == (sort_column_to_toggle as usize) {
                if request.sort_asc.unwrap_or(true) {
                    url.push_str(&false.to_string());
                } else {
                    url.push_str(&true.to_string());
                }
                return url;
            } else {
                url.push_str(&false.to_string());
            }
        }
    }

    url
}
