//! 
//! Known limitations:
//!  - It currently doesn't support redirects. This issue will be resolved when awc::Client supports it.
//!    (it claims that it does, but it really doesn't: https://github.com/actix/actix-web/issues/1571)
//!  - WebApi is not encrypted 
//! 
use actix_web::{App, HttpResponse, HttpServer, Responder, get, middleware::Logger, web::{self, BufMut}};
use env_logger::Env;
use futures::prelude::*;

mod crawler;

#[derive(serde::Serialize)]
struct InvalidUrlError {
    invalid_url: String
}

#[get("/crawler/domain/{domain}/list_successful")]
async fn list(domain: web::Path<String>, state: web::Data<AppState>) -> impl Responder {
    let mut input = domain.into_inner();
    input.insert_str(0, "https://"); // yes, https only unless PO requests otherwise :-)
    let url = url::Url::parse(&input)
        .map_err(|_| HttpResponse::BadRequest()
            .json(InvalidUrlError { invalid_url: input }))?;
    
    HttpResponse::Ok().streaming::<_, actix_web::Error>(state
        .crawler.create(url)
        .filter(|v| future::ready(v.result.is_ok()))
        .map(|visit| {
            let mut buf = actix_web::web::BytesMut::new();
            buf.put(visit.url.as_str().as_bytes());
            buf.put_u8(b'\n');
            Ok(buf.into())
        })).await
}

#[get("/crawler/domain/{domain}/count")]
async fn count(domain: web::Path<String>, state: web::Data<AppState>) -> Result<String, actix_web::Error> {
    let url = parse_url(domain.into_inner())?;
    
    Ok(state.crawler.create(url)
        .filter(|v| future::ready(v.result.is_ok()))
        .fold(0, |acc, _x| async move { acc + 1 })
        .await
        .to_string()
    )
}

fn parse_url(mut input: String) -> Result<url::Url, actix_web::Error> {
    input.insert_str(0, "https://"); // yes, https only unless PO requests otherwise :-)
    Ok(url::Url::parse(&input)
        .map_err(|_| HttpResponse::BadRequest()
            .json(InvalidUrlError { invalid_url: input }))?)
}

struct AppState {
    pub crawler: crawler::CrawlerStreamFactory
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
            .service(list)
            .service(count)
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
    async fn test_invalid_url_fails() {    
        let mut app = test::init_service(App::new()
            .data(AppState {
                crawler: crawler::CrawlerStreamFactory::new_rustls(),
            })        
            .service(list)
            .service(count)
        ).await;  
 
        let req = test::TestRequest::get().uri("/crawler/domain/i%nvalid/list_successful").to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(400, resp.status());

        let req = test::TestRequest::get().uri("/crawler/domain/i%nvalid/count").to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(400, resp.status());
    }
}