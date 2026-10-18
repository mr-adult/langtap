use std::process::ExitCode;
use std::sync::Arc;

use axum::http::Uri;
use axum::{
    Router,
    routing::{any, get},
};
use clap::{Arg, command};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::net::TcpListener;
use tracing_subscriber::{
    EnvFilter,
    {layer::SubscriberExt, util::SubscriberInitExt},
};

use crate::error_logging::PostgresLayer;

mod error_logging;
mod html;
mod lsp;
mod message;
mod replay_client;
mod session;
mod utils;

#[derive(Clone)]
struct AppState {
    db: PgPool,
    forward_url: Arc<Uri>,
}

#[tokio::main]
pub async fn main() -> ExitCode {
    let matches = command!()
        .about(
            r#"
  _                     _____
 | |    __ _ _ __   __ |_   _|_ _ _ __ 
 | |   / _` | '_ \ / _` || |/ _` | '_ \
 | |__| (_| | | | | (_| || | (_| | |_) |
 |_____\__,_|_| |_|\__, ||_|\__,_| .__/
                   |___/         |_|"#,
        )
        .arg(
            Arg::new("ws_port")
                .help("the port to listen for websocket connections on")
                .long("ws_port"),
        )
        .arg(
            Arg::new("http_port")
                .help("the port to listen for website traffic on")
                .long("http_port"),
        )
        .arg(
            Arg::new("forward_url")
                .help("the URL of the actual language server - traffic will be forwarded there")
                .long("forward_url"),
        )
        .arg(
            Arg::new("DATABASE_URL")
                .help("the URL of the postgres database instance")
                .long("db"),
        )
        .get_matches();

    println!("Starting Up...");

    // First, parse the .env file for our environment setup.
    dotenvy::dotenv().ok();

    let ws_port = if let Some(ws_port) = matches.get_one::<String>("ws_port") {
        ws_port.to_string()
    } else if let Ok(ws_port) = std::env::var("ws_port") {
        ws_port
    } else {
        println!("No ws_port was specified.");
        return ExitCode::FAILURE;
    };

    let ws_port = match ws_port.parse::<u16>() {
        Ok(ws_port) => ws_port,
        Err(_) => {
            println!("Failed to parse ws_port as u16.");
            return ExitCode::FAILURE;
        }
    };

    let http_port = if let Some(http_port) = matches.get_one::<String>("http_port") {
        http_port.to_string()
    } else if let Ok(http_port) = std::env::var("http_port") {
        http_port
    } else {
        println!("No http_port was specified.");
        return ExitCode::FAILURE;
    };

    let http_port = match http_port.parse::<u16>() {
        Ok(http_port) => http_port,
        Err(_) => {
            println!("Failed to parse http_port as u16.");
            return ExitCode::FAILURE;
        }
    };

    let forward_url = matches
        .get_one::<String>("forward_url")
        .map(String::to_string)
        .unwrap_or_else(|| std::env::var("forward_url").expect("No forward_url was specified."))
        .parse::<Uri>()
        .expect("Failed to parse forward_url as URI");

    let database_url = matches
        .get_one::<String>("DATABASE_URL")
        .map(String::to_string)
        .unwrap_or_else(|| {
            std::env::var("DATABASE_URL")
                .expect("DATABASE_URL must be set either via command line argument or .env file.")
        });

    // We create a single connection pool for SQLx that's shared across the whole application.
    // This saves us from opening a new connection for every API call, which is wasteful.
    let pool = PgPoolOptions::new()
        // The default connection limit for a Postgres server is 100 connections, minus 3 for superusers.
        // We should leave some connections available for manual access.
        //
        // If you're deploying your application with multiple replicas, then the total
        // across all replicas should not exceed the Postgres connection limit.
        .max_connections(10)
        .connect(&database_url)
        .await
        .unwrap_or_else(|err| panic!("Could not connect to dabase_url. Error: \n{}", err));

    // Run any SQL migrations to get the DB into the correct state
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .unwrap_or_else(|err| panic!("Failed to migrate the database. Error: \n{}", err));

    tracing_subscriber::registry()
        .with(
            EnvFilter::from_default_env()
                // this directive prevent sqlx from infinitely logggin its own events.
                .add_directive("langtap".parse().unwrap()),
        )
        .with(PostgresLayer::from(pool.clone()))
        .init();

    let app_state = AppState {
        db: pool,
        forward_url: Arc::new(forward_url),
    };

    let ws_router = Router::new()
        .route("/", any(lsp::handle_ws))
        // FUTURE: handle regular POST requests. Need to create an API to retrieve a session ID first.
        // .route("/log", post(handle_log))
        .with_state(app_state.clone())
        .into_make_service();

    let http_router = Router::new()
        .route("/", get(html::session_search::get_sessions))
        .route("/replay", get(replay_client::handle_replay))
        .route("/session", get(html::get_session))
        // FUTURE: handle regular POST requests. Need to create an API to retrieve a session ID first.
        // .route("/log", post(handle_log))
        .with_state(app_state)
        .into_make_service();

    let (ws_listener_result, http_listener_result) = tokio::join! {
        TcpListener::bind(format!("[::]:{ws_port}")),
        TcpListener::bind(format!("[::]:{http_port}"))
    };

    let ws_listener = ws_listener_result.expect(&format!("failed to bind to [::]:{ws_port}"));
    let http_listener = http_listener_result.expect(&format!("failed to bind to [::]:{ws_port}"));

    println!("Listening for websocket connections on: [::]:{ws_port}");
    println!("Listening for http connections on: [::]:{http_port}");

    let ws_future = axum::serve(ws_listener, ws_router);
    let http_future = axum::serve(http_listener, http_router);

    tokio::select! {
        value = ws_future => {
            value
        }
        value = http_future => {
            value
        }
    }
    .expect("Failed to start service");

    ExitCode::SUCCESS
}
