use std::{
    collections::HashMap,
    io::{Read, Write},
    net::TcpListener,
    thread::{self, JoinHandle},
    sync::Arc,
};

use arrayvec::ArrayString;
use bytes::Bytes;
use databus::{message::MessageType, producer::Producer};
use producers::html_list_producer::{HtmlListProducer, HtmlListProducerState};

fn topic(s: &str) -> ArrayString<20> {
    ArrayString::from(s).unwrap()
}

fn listing_page(base_url: &str) -> String {
    format!(
        r#"
        <html>
            <body>
                <ul class="list-class">
                    <li><a class="link-class" href="{base_url}/dataset-1.csv">One</a></li>
                    <li><a class="link-class" href="{base_url}/dataset-2.csv">Two</a></li>
                </ul>
            </body>
        </html>
        "#
    )
}

fn start_test_server<F>(build_routes: F, expected_requests: usize) -> (String, JoinHandle<()>)
where
    F: FnOnce(&str) -> HashMap<String, String> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
    let base_url = format!(
        "http://{}",
        listener.local_addr().expect("server local_addr")
    );
    let routes = build_routes(&base_url);

    let handle = thread::spawn(move || {
        for _ in 0..expected_requests {
            let (mut stream, _) = listener.accept().expect("accept connection");
            let mut buffer = [0_u8; 4096];
            let bytes_read = stream.read(&mut buffer).expect("read request");
            let request = String::from_utf8_lossy(&buffer[..bytes_read]);
            let path = request
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .expect("request path");

            let (status, body) = match routes.get(path) {
                Some(body) => ("200 OK", body.as_str()),
                None => ("404 Not Found", ""),
            };

            write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .expect("write response");
            stream.flush().expect("flush response");
        }
    });

    (base_url, handle)
}

#[tokio::test]
async fn produce_fetches_first_dataset_and_updates_state() {
    let (base_url, server) = start_test_server(
        |base_url| {
            HashMap::from([
                ("/".to_string(), listing_page(base_url)),
                ("/dataset-1.csv".to_string(), "first-dataset".to_string()),
            ])
        },
        2,
    );

    let producer = HtmlListProducer::new(&base_url, "ul.list-class", "a.link-class", false)
        .expect("create producer");
    let initial_state = HtmlListProducerState {
        last_extracted_url: None,
    };

    let (message, new_state) = producer.produce(topic("html.list"), initial_state).await;

    assert_eq!(*message.message_type(), MessageType::Data);
    assert_eq!(*message.payload(), Bytes::from("first-dataset"));
    assert_eq!(
        message.meta_by_key("filename").unwrap(),
        "dataset-1.csv"
    );
    assert_eq!(
        new_state.last_extracted_url,
        Some(format!("{base_url}/dataset-1.csv"))
    );

    server.join().expect("join test server");
}

#[tokio::test]
async fn produce_uses_checkpoint_to_fetch_next_dataset() {
    let (base_url, server) = start_test_server(
        |base_url| {
            HashMap::from([
                ("/".to_string(), listing_page(base_url)),
                ("/dataset-2.csv".to_string(), "second-dataset".to_string()),
            ])
        },
        2,
    );

    let producer = HtmlListProducer::new(&base_url, "ul.list-class", "a.link-class", false)
        .expect("create producer");
    let initial_state = HtmlListProducerState {
        last_extracted_url: Some(format!("{base_url}/dataset-1.csv")),
    };

    let (message, new_state) = producer.produce(topic("html.list"), initial_state).await;

    assert_eq!(*message.message_type(), MessageType::Data);
    assert_eq!(*message.payload(), Bytes::from("second-dataset"));
    assert_eq!(
        message.meta_by_key("filename").unwrap(),
        "dataset-2.csv"
    );
    assert_eq!(
        new_state.last_extracted_url,
        Some(format!("{base_url}/dataset-2.csv"))
    );

    server.join().expect("join test server");
}

#[tokio::test]
async fn produce_respects_ingest_from_back() {
    let (base_url, server) = start_test_server(
        |base_url| {
            HashMap::from([
                ("/".to_string(), listing_page(base_url)),
                ("/dataset-2.csv".to_string(), "latest-dataset".to_string()),
            ])
        },
        2,
    );

    let producer = HtmlListProducer::new(&base_url, "ul.list-class", "a.link-class", true)
        .expect("create producer");
    let initial_state = HtmlListProducerState {
        last_extracted_url: None,
    };

    let (message, new_state) = producer.produce(topic("html.list"), initial_state).await;

    assert_eq!(*message.message_type(), MessageType::Data);
    assert_eq!(*message.payload(), Bytes::from("latest-dataset"));
    assert_eq!(
        message.meta_by_key("filename").unwrap(),"dataset-2.csv"
    );
    assert_eq!(
        new_state.last_extracted_url,
        Some(format!("{base_url}/dataset-2.csv"))
    );

    server.join().expect("join test server");
}

#[tokio::test]
async fn produce_returns_empty_when_checkpoint_is_latest_link() {
    let (base_url, server) = start_test_server(
        |base_url| HashMap::from([("/".to_string(), listing_page(base_url))]),
        1,
    );

    let producer = HtmlListProducer::new(&base_url, "ul.list-class", "a.link-class", false)
        .expect("create producer");
    let initial_state = HtmlListProducerState {
        last_extracted_url: Some(format!("{base_url}/dataset-2.csv")),
    };

    let expected_last_url = initial_state.last_extracted_url.clone();
    let (message, new_state) = producer.produce(topic("html.list"), initial_state).await;

    assert_eq!(*message.message_type(), MessageType::Empty);
    assert_eq!(*message.payload(), Bytes::new());
    assert!(Arc::into_inner(message).unwrap().into_meta().is_none());
    assert_eq!(new_state.last_extracted_url, expected_last_url);

    server.join().expect("join test server");
}
