use {
    actix_web::{
        get,
        middleware::Logger,
        web::{self, BufMut},
        App, HttpResponse, HttpServer, Responder,
    },
    env_logger::Env,
    futures::prelude::*,
    std::fmt::Write,
};

mod crawler;

#[derive(serde::Serialize)]
struct InvalidUrlError {
    invalid_url: String,
}

#[get("/crawler/domain/{domain}/ok")]
async fn ok(domain: web::Path<String>, state: web::Data<AppState>) -> impl Responder {
    let url = parse_url(domain.into_inner())?;

    HttpResponse::Ok()
        .streaming::<_, actix_web::Error>(
            state
                .crawler
                .create(url)
                .filter(|v| future::ready(v.is_ok()))
                .map(|visit| {
                    let mut buf = actix_web::web::BytesMut::new();
                    buf.put(visit.url.as_str().as_bytes());
                    buf.put_u8(b'\n');
                    Ok(buf.into())
                }),
        )
        .await
}

// Pretty nasty, but I haven't found a better solution for streaming json yet.
#[get("/crawler/domain/{domain}/problems")]
async fn problems(domain: web::Path<String>, state: web::Data<AppState>) -> impl Responder {
    let url = parse_url(domain.into_inner())?;
    let mut is_first = true;
    let element_stream = state
        .crawler.create(url)
        .filter(|v| future::ready(!v.problems.is_empty()))
        .map(move |visit| {
            let mut buf = actix_web::web::BytesMut::new();
            if is_first {
                is_first = false
            } else {
                buf.put_u8(b',');
            }

            if let Ok(str) = serde_json::to_string(&visit) {
                if !buf.write_str(&str).is_ok() {
                    log::error!("Failed to write JSON for url {} to the buffer", visit.url.as_str());
                }
            } else {
                log::error!("Failed to convert to JSON. Result for url {} is therefore not sent to the output", visit.url.as_str());
            }

            Ok(buf.into())
        });
    let start = stream::once(future::ready(Ok(actix_web::web::Bytes::from_static(
        &b"["[..],
    ))));
    let end = stream::once(future::ready(Ok(actix_web::web::Bytes::from_static(
        &b"]"[..],
    ))));
    HttpResponse::Ok()
        .content_type("application/json")
        .streaming::<_, actix_web::Error>(start.chain(element_stream).chain(end))
        .await
}

#[get("/crawler/domain/{domain}/ok/count")]
async fn count_ok(
    domain: web::Path<String>,
    state: web::Data<AppState>,
) -> Result<String, actix_web::Error> {
    let url = parse_url(domain.into_inner())?;

    Ok(state
        .crawler
        .create(url)
        .filter(|v| future::ready(v.is_ok()))
        .fold(0, |acc, _x| async move { acc + 1 })
        .await
        .to_string())
}

#[get("/crawler/domain/{domain}/problems/count")]
async fn count_problems(
    domain: web::Path<String>,
    state: web::Data<AppState>,
) -> Result<String, actix_web::Error> {
    let url = parse_url(domain.into_inner())?;

    Ok(state
        .crawler
        .create(url)
        .fold(0, |acc, x| async move { acc + x.problems.len() })
        .await
        .to_string())
}

fn parse_url(mut input: String) -> Result<url::Url, actix_web::Error> {
    input.insert_str(0, "https://"); // yes, https only unless PO requests otherwise :-)
    Ok(url::Url::parse(&input)
        .map_err(|_| HttpResponse::BadRequest().json(InvalidUrlError { invalid_url: input }))?)
}

struct AppState {
    pub crawler: crawler::CrawlerStreamFactory,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    HttpServer::new(|| {
        App::new()
            .data(AppState {
                crawler: crawler::CrawlerStreamFactory::new_rustls(),
            })
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
            .service(ok)
            .service(count_ok)
            .service(problems)
            .service(count_problems)
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await
}
#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test, App};

    #[actix_rt::test]
    async fn request_invalid_url_returns_400_status() {
        let mut app = test::init_service(
            App::new()
                .data(AppState {
                    crawler: crawler::CrawlerStreamFactory::new_rustls(),
                })
                .service(ok)
                .service(count_ok)
                .service(problems)
                .service(count_problems),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/crawler/domain/i%nvalid/ok")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(400, resp.status());

        let req = test::TestRequest::get()
            .uri("/crawler/domain/i%nvalid/ok/count")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(400, resp.status());

        let req = test::TestRequest::get()
            .uri("/crawler/domain/i%nvalid/problems")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(400, resp.status());

        let req = test::TestRequest::get()
            .uri("/crawler/domain/i%nvalid/problems/count")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(400, resp.status());
    }
}
