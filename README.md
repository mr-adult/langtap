# Langtap

Langtap is a language server proxy intended to intercept traffic between your language server and language client. It will log and maintain a PostgreSQL database of every request that it receives, along with any protocol errors its validation catches. This lets you stand up a language server observability solution without building all of the logging infrastucture yourself.

Langtap also offers some notable language server debugging features. These include:
- protocol verification
    - langtap logs all protocol violations and will even intervene if traffic is breaking LSP spec compliance. For example, if a client sends a request that is not permitted before the initialize handshake, langtap will intervene and respond with a server not initialized error code.
- language server protocol conversation replay
    - langtap can replay a conversation back to the language server to replicate bugs.

![a screenshot of the langtap messages interface](langtap_ui.png)

## Getting Started

### Docker Setup

Langtap supports dockerization, so the easiest way to get started is to stand up a docker instance. You can find an example docker-compose.yml located at ./docker-compose.yml. You can use this compose file as-is, but you may want to consider changing the port mappings to something easier to remember. You will also need to create a .env file modelled after `.env.example`.

Once you have your .env file configured, run `docker compose up` to spin up the API server and PostgreSQL database. When you first launch, the API server will fail to boot. This is because you need to create its `langtap` database.
