//! Utilities to asynchronously stream JSON in actix
//!

use {
    actix_http::{Response, ResponseBuilder},
    actix_web::web::Bytes,
    futures::{Stream, StreamExt},
    serde::Serialize,
    std::fmt::Debug,
};

/// https://en.wikipedia.org/wiki/Maximum_transmission_unit#Ethernet_maximum_frame_size
const ETHERNET_MAX_FRAME_PAYLOAD_SIZE: usize = 1500;

#[derive(Debug)]
pub struct StreamJsonSettings {
    output_buffer_size: usize,
}

impl Default for StreamJsonSettings {
    fn default() -> Self {
        Self {
            output_buffer_size: ETHERNET_MAX_FRAME_PAYLOAD_SIZE,
        }
    }
}

pub trait ActixStreamJson {
    /// Streams the items with default settings
    fn stream_json<TItem, TStream>(self, items: TStream) -> Response
    where
        TItem: Serialize + Debug + 'static,
        TStream: Stream<Item = TItem> + Unpin + 'static;

    /// Streams the items using the provided settings
    fn stream_json_with_settings<TItem, TStream>(
        self,
        items: TStream,
        settings: StreamJsonSettings,
    ) -> Response
    where
        TItem: Serialize + Debug + 'static,
        TStream: Stream<Item = TItem> + Unpin + 'static;
}

impl ActixStreamJson for ResponseBuilder {
    fn stream_json<TItem, TStream>(self, items: TStream) -> Response
    where
        TItem: Serialize + Debug + 'static,
        TStream: Stream<Item = TItem> + Unpin + 'static,
    {
        self.stream_json_with_settings(items, StreamJsonSettings::default())
    }

    fn stream_json_with_settings<TItem, TStream>(
        mut self,
        items: TStream,
        settings: StreamJsonSettings,
    ) -> Response
    where
        TItem: Serialize + Debug + 'static,
        TStream: Stream<Item = TItem> + Unpin + 'static,
    {
        let byte_stream = create_bytes_stream(items, settings);
        self.set_header(actix_http::http::header::CONTENT_TYPE, "application/json");
        self.streaming::<_, serde_json::Error>(byte_stream.boxed_local())
    }
}

fn create_bytes_stream(
    mut input: impl Stream<Item = impl Serialize + Debug> + Unpin,
    settings: StreamJsonSettings,
) -> impl Stream<Item = Result<Bytes, serde_json::Error>> {
    let mut buf = Vec::with_capacity(settings.output_buffer_size);
    buf.push(b'[');

    async_stream::stream! {
        if let Some(item) = input.next().await {
            if let Err(e) = serde_json::to_writer(&mut buf, &item) {
                yield Err(e);
            }
            loop {
                for chunk in FullChunkConsumer(&mut buf, settings.output_buffer_size) {
                    // Vec<u8>::into::<Bytes>() is O(1)
                    yield Ok(chunk.into());
                }

                match input.next().await {
                    Some(item) => {
                        buf.push(b',');
                        if let Err(e) = serde_json::to_writer(&mut buf, &item) {
                            yield Err(e);
                        }
                    },
                    None => break
                }
            }
        }

        buf.push(b']');
        for chunk in FullChunkConsumer(&mut buf, settings.output_buffer_size) {
            yield Ok(chunk.into());
        }

        yield Ok(buf.into())
    }
}

/// Consumes all chunks of size self.1 and keeps the remainings in the input
struct FullChunkConsumer<'a, T>(&'a mut Vec<T>, usize);
impl<'a, T> Iterator for FullChunkConsumer<'a, T> {
    type Item = Vec<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.len() > self.1 {
            let mut rest = self.0.split_off(self.1);
            std::mem::swap(&mut rest, &mut self.0);
            Some(rest)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        actix_web::{
            dev::{Body, ServiceResponse},
            test,
            web::{self, Buf, BufMut, Bytes, BytesMut},
            App, HttpResponse, Responder,
        },
    };

    async fn empty() -> impl Responder {
        HttpResponse::Ok().stream_json(futures::stream::empty::<i32>())
    }

    async fn one() -> impl Responder {
        HttpResponse::Ok().stream_json(futures::stream::iter(vec![42i32]))
    }

    async fn two() -> impl Responder {
        HttpResponse::Ok().stream_json(futures::stream::iter(vec!["42", "43"]))
    }

    async fn reduced_buffer() -> impl Responder {
        HttpResponse::Ok().stream_json_with_settings(
            futures::stream::iter(vec![42i32, 43, 44]),
            StreamJsonSettings {
                output_buffer_size: 4,
            },
        )
    }

    #[actix_rt::test]
    async fn integration_test() {
        let mut app = test::init_service(
            App::new()
                .route("/empty", web::get().to(empty))
                .route("/one", web::get().to(one))
                .route("/two", web::get().to(two))
                .route("/reduced_buffer", web::get().to(reduced_buffer)),
        )
        .await;

        let configs = [
            ("/empty", "[]", 1),
            ("/one", "[42]", 1),
            ("/two", "[\"42\",\"43\"]", 1),
            ("/reduced_buffer", "[42,43,44]", 3),
        ];

        for (url, expected, expected_chunks) in configs.iter() {
            let request = test::TestRequest::get().uri(*url).to_request();
            let response = test::call_service(&mut app, request).await;
            assert_eq!(200, response.status());
            let headers = response.headers();
            assert_eq!(
                "application/json",
                headers
                    .get(actix_http::http::header::CONTENT_TYPE)
                    .expect("Expected to send a content-type")
            );

            assert_eq!(
                Bytes::from(*expected),
                read_body(response, *expected_chunks).await
            );
        }
    }

    async fn read_body(mut response: ServiceResponse<Body>, expected_chunks: usize) -> BytesMut {
        let mut output = BytesMut::new();
        let (body, mut resp) = response.take_body().into_future().await;
        collect(body, &mut output);

        for _ in 0..expected_chunks - 1 {
            let (body, next_resp) = resp.take_body().into_future().await;
            resp = next_resp;
            collect(body, &mut output)
        }

        output
    }

    fn collect(body: Option<Result<Bytes, actix_web::Error>>, output: &mut BytesMut) {
        if let Some(Ok(x)) = body {
            output.put(x.bytes());
        } else {
            panic!("Got no more body after: {:?}", output);
        }
    }
}
