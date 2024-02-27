use anyhow::{Context, Result};
use bytes::Bytes;
use coap::Server;
use coap_lite::{CoapRequest, RequestType as Method};
use std::net::SocketAddr;
use tracing::{debug, error, field::debug, info, instrument, trace, Span};
use url::Url;

const LISTEN_ADDRESS: &str = "127.0.0.1:5683";
const HOMESEVER: &str = "http://localhost:8015/";

#[instrument(
    skip_all,
    fields(
        coap_method = ?request.get_method(),
        coap_path = request.get_path(),
        http_url, 
        http_status,
    )
)]
async fn request_handler(
    mut request: Box<CoapRequest<SocketAddr>>,
) -> Box<CoapRequest<SocketAddr>> {
    let client = reqwest::Client::new();

    info!("Received a CoAP request");

    let method = match request.get_method() {
        Method::Get => reqwest::Method::GET,
        Method::Post => reqwest::Method::POST,
        Method::Put => reqwest::Method::PUT,
        Method::Delete => reqwest::Method::DELETE,
        Method::Patch => reqwest::Method::PATCH,
        m => unimplemented!("Method {m:?} is not supported"),
    };

    let url = HOMESEVER.to_owned() + &request.get_path();
    let mut url = Url::parse(&url).unwrap();

    let access_token: Option<String>;

    // Let's move the access token out of the query string so we can put it into a header.
    {
        let query_pairs: Vec<(String, String)>;

        let pairs = url.query_pairs();
        access_token = pairs
            .into_iter()
            .find(|(key, _)| key == "access_token")
            .map(|(_, value)| "Bearer ".to_owned() + &value);

        query_pairs = pairs
            .filter(|(key, _)| key != "access_token")
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        let mut mut_query_pairs = url.query_pairs_mut();
        mut_query_pairs.clear();
        mut_query_pairs.extend_pairs(query_pairs).finish();
    }

    Span::current().record("http_url", debug(&url));

    // TODO: If we start to transfer the body as cbor, convert it to JSON for the homeserver.
    let body = Bytes::from(request.message.payload.clone());

    let request_builder = client
        .request(method, url)
        .body(body)
        .header("Content-type", "application/json");

    let request_builder = if let Some(access_token) = access_token {
        request_builder.header("Authorization", access_token)
    } else {
        request_builder
    };

    trace!("Built the HTTP request");

    match request_builder.send().await {
        Ok(response) => {
            Span::current().record("http_status", debug(response.status()));

            debug!("Successfully sent the HTTP response");

            match response.bytes().await {
                Ok(body) => {
                    if let Some(message) = &mut request.response {
                        trace!("Setting the CoAP response");

                        // TODO: If we start to transfer the body as cbor, convert the JSON
                        // response into cbor.
                        message.set_status(coap_lite::ResponseType::Valid);
                        message.message.payload = body.to_vec();
                    }
                }
                Err(e) => {
                    error!("Could not send out the HTTP request {e}")
                }
            }
        }
        Err(e) => {
            error!("Could not send out the HTTP request {e}")
        }
    }

    info!("Forwarded the request to the homeserver, replying to the client");

    return request;
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().pretty().init();

    let server = Server::new_udp(LISTEN_ADDRESS).context("Could not start up the server")?;
    info!(
        listen_address = LISTEN_ADDRESS,
        homeserver_address = HOMESEVER,
        "Server up"
    );

    server
        .run(request_handler)
        .await
        .context("Failed to run the CoAP server")?;

    Ok(())
}
