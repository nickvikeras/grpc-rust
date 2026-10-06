//! Frames that arrive for streams the client has already reset, and the `Endpoint` settings that
//! decide whether they close the connection.

use std::{net::SocketAddr, pin::Pin, time::Duration};

use integration_tests::pb::{
    Input1, Output1,
    test1_client::Test1Client,
    test1_server::{Test1, Test1Server},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::Instant,
};
use tokio_stream::{Stream, StreamExt};
use tonic::{
    Code, Request, Response, Status,
    transport::{Endpoint, Server, server::TcpIncoming},
};

/// How long the proxy holds client-to-server bytes. The server keeps streaming into a stream the
/// client has reset until the reset reaches it, so the client receives frames for the stream after
/// resetting it.
const CLIENT_TO_SERVER_DELAY: Duration = Duration::from_millis(20);

/// Streams 1 KiB messages until the client goes away: as fast as it can if the request's `buf` is
/// non-empty, otherwise one every 5 ms.
struct Svc;

#[tonic::async_trait]
impl Test1 for Svc {
    async fn unary_call(&self, _: Request<Input1>) -> Result<Response<Output1>, Status> {
        Ok(Response::new(Output1::default()))
    }

    type StreamCallStream = Pin<Box<dyn Stream<Item = Result<Output1, Status>> + Send>>;

    async fn stream_call(
        &self,
        request: Request<Input1>,
    ) -> Result<Response<Self::StreamCallStream>, Status> {
        let fast = !request.into_inner().buf.is_empty();
        let messages = tokio_stream::iter(std::iter::repeat_with(|| {
            Ok(Output1 { buf: vec![0; 1024] })
        }));
        let stream: Self::StreamCallStream = if fast {
            Box::pin(messages)
        } else {
            Box::pin(messages.throttle(Duration::from_millis(5)))
        };
        Ok(Response::new(stream))
    }
}

/// Starts the server behind a proxy that delays client-to-server bytes by
/// `CLIENT_TO_SERVER_DELAY`, and returns an `Endpoint` for the proxy.
async fn endpoint() -> Endpoint {
    let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = server.local_addr().unwrap();
    tokio::spawn(
        Server::builder()
            .add_service(Test1Server::new(Svc))
            .serve_with_incoming(TcpIncoming::from(server)),
    );

    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((client, _)) = proxy.accept().await {
            tokio::spawn(forward(client, server_addr));
        }
    });

    Endpoint::from_shared(format!("http://{proxy_addr}"))
        .unwrap()
        // Large windows, so that flow control never stops the server streaming.
        .initial_stream_window_size(1 << 30)
        .initial_connection_window_size(1 << 30)
}

async fn forward(client: TcpStream, server_addr: SocketAddr) {
    let server = TcpStream::connect(server_addr).await.unwrap();
    let (mut client_read, mut client_write) = client.into_split();
    let (mut server_read, mut server_write) = server.into_split();
    tokio::spawn(async move { tokio::io::copy(&mut server_read, &mut client_write).await });

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(Instant, Vec<u8>)>();
    tokio::spawn(async move {
        while let Some((due, bytes)) = rx.recv().await {
            tokio::time::sleep_until(due).await;
            if server_write.write_all(&bytes).await.is_err() {
                break;
            }
        }
    });
    let mut buf = vec![0; 64 * 1024];
    while let Ok(n @ 1..) = client_read.read(&mut buf).await {
        let due = Instant::now() + CLIENT_TO_SERVER_DELAY;
        if tx.send((due, buf[..n].to_vec())).is_err() {
            break;
        }
    }
}

/// Opens a slow stream, then cancels up to `cancels` fast streams on the same connection, one at a
/// time. Returns the slow stream's error once it fails, or `None` if it outlives every cancel and a
/// grace period after the last one.
async fn cancel_streams(endpoint: Endpoint, cancels: usize) -> Option<Status> {
    let mut client = Test1Client::new(endpoint.connect().await.unwrap());
    let mut slow = client
        .stream_call(Input1::default())
        .await
        .unwrap()
        .into_inner();

    // Read the slow stream as messages arrive: messages already buffered would be returned before
    // the connection error.
    let (slow_error_tx, mut slow_error) = oneshot::channel();
    let slow_reader = tokio::spawn(async move {
        let status = loop {
            match slow.message().await {
                Ok(Some(_)) => {}
                Ok(None) => break Status::internal("slow stream ended"),
                Err(status) => break status,
            }
        };
        let _ = slow_error_tx.send(status);
    });

    for _ in 0..cancels {
        let mut fast = client
            .stream_call(Input1 { buf: vec![1] })
            .await
            .unwrap()
            .into_inner();
        fast.message().await.unwrap();
        drop(fast);

        tokio::time::sleep(3 * CLIENT_TO_SERVER_DELAY).await;
        if let Ok(status) = slow_error.try_recv() {
            return Some(status);
        }
    }

    // On a busy machine the last cancel's late frames, and so the connection error, can arrive
    // after the check above.
    let late_error = tokio::time::timeout(Duration::from_secs(2), &mut slow_error).await;
    slow_reader.abort();
    late_error.ok().and_then(Result::ok)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_frames_for_forgotten_streams_count_toward_the_limit() {
    let endpoint = endpoint()
        .await
        .http2_max_concurrent_reset_streams(0)
        .http2_max_local_error_reset_streams(Some(0));

    // One cancel: hyper's default limit takes many more late frames than one cancel produces.
    let status = cancel_streams(endpoint, 1)
        .await
        .expect("connection closed");
    assert_eq!(status.code(), Code::ResourceExhausted, "{status:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_frames_for_remembered_streams_are_ignored() {
    let endpoint = endpoint()
        .await
        .http2_max_concurrent_reset_streams(100)
        .http2_reset_stream_duration(Duration::from_secs(60))
        .http2_max_local_error_reset_streams(Some(0));

    let error = cancel_streams(endpoint, 5).await;
    assert!(error.is_none(), "{error:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reset_streams_are_forgotten_after_the_duration() {
    let endpoint = endpoint()
        .await
        .http2_reset_stream_duration(Duration::ZERO)
        .http2_max_local_error_reset_streams(Some(0));

    let status = cancel_streams(endpoint, 1)
        .await
        .expect("connection closed");
    assert_eq!(status.code(), Code::ResourceExhausted, "{status:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removing_the_limit_keeps_the_connection_open() {
    let endpoint = endpoint()
        .await
        .http2_max_concurrent_reset_streams(0)
        .http2_max_local_error_reset_streams(None);

    let error = cancel_streams(endpoint, 20).await;
    assert!(error.is_none(), "{error:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_limit_applies_when_not_set() {
    let endpoint = endpoint().await.http2_max_concurrent_reset_streams(0);

    let status = cancel_streams(endpoint, 200)
        .await
        .expect("connection closed");
    assert_eq!(status.code(), Code::ResourceExhausted, "{status:?}");
}
