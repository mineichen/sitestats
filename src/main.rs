use {
    actix_extensions::ActixStreamJson,
    actix_web::{
        get,
        middleware::Logger,
        web::{self, BufMut},
        App, HttpResponse, HttpServer, Responder,
    },
    env_logger::Env,
    futures::prelude::*,
};

mod actix_extensions;
mod crawler;

#[derive(serde::Serialize)]
struct InvalidUrlError {
    invalid_url: String,
}

#[get("/crawler/domain/{domain}/ok")]
async fn ok(domain: web::Path<String>, state: web::Data<AppState>) -> impl Responder {
    let url = create_url_from_domain(domain.into_inner())?;

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

#[get("/crawler/domain/{domain}/problems")]
async fn problems(
    domain: web::Path<String>,
    state: web::Data<AppState>,
) -> actix_web::Result<impl Responder> {
    let url = create_url_from_domain(domain.into_inner())?;
    Ok(HttpResponse::Ok().stream_json(
        state
            .crawler
            .create(url)
            .filter(|v| future::ready(!v.problems.is_empty())),
    ))
}

#[get("/crawler/domain/{domain}/ok/count")]
async fn count_ok(
    domain: web::Path<String>,
    state: web::Data<AppState>,
) -> Result<String, actix_web::Error> {
    let url = create_url_from_domain(domain.into_inner())?;

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
    let url = create_url_from_domain(domain.into_inner())?;

    Ok(state
        .crawler
        .create(url)
        .fold(0, |acc, x| async move { acc + x.problems.len() })
        .await
        .to_string())
}

fn create_url_from_domain(mut input: String) -> Result<url::Url, actix_web::Error> {
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
