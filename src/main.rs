use actix_web::{App, HttpResponse, HttpServer, Responder, get, middleware::Logger, web};
use env_logger::Env;
use log::{error};
use futures::prelude::*;

mod crawler;

#[derive(serde::Serialize)]
struct InvalidUrlError {
    invalid_url: String
}

#[get("/crawler/domain/{domain}")]
async fn index(domain: web::Path<String>, state: web::Data<AppState>) -> impl Responder {
    let mut url_str = domain.into_inner();
    url_str.insert_str(0, "https://"); // yes, https only unless PO requests otherwise :-)
    let url = url::Url::parse(&url_str)
        .map_err(|_| HttpResponse::BadRequest().json(InvalidUrlError { invalid_url: url_str }))?;

    let stream = state.crawler.create(url);
    
    HttpResponse::Ok().streaming::<_, actix_web::Error>(stream.map(|visit| {
        let mut buf = actix_web::web::BytesMut::new();
        use std::fmt::Write;
        writeln!(&mut buf, "{}", visit.url).map_err(|e| {
            error!("{}", e);
            HttpResponse::InternalServerError().finish()
        })?;
        
        Ok(buf.into())
    })).await
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
            .service(index)
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await
}
