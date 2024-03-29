//! Lacking of a good/maintained crate alternative.
//! - Quick_crawler:0.1.2: the best candidate found, still very young ( during evaluation), no recursion support, aborts on error
//! - url-crawler:0.3.0: uses threads. While usable for a few users, it requires too many OS threads to be used within a web api.                     

use std::collections::HashMap;

use {
    actix_web::{
        error::PayloadError,
        web::{Buf, Bytes},
    },
    futures::{future::LocalBoxFuture, prelude::*, Stream},
    scraper::{Html, Selector},
    std::{
        collections::HashSet,
        marker::PhantomData,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
    },
    url::Url,
};

type CrawlJob = LocalBoxFuture<'static, CrawlerStreamResult>;

struct HttpResponse {
    headers: HashMap<String, Vec<u8>>,
    status: u16,
    request_url: Url,
    body_future: LocalBoxFuture<'static, Result<Bytes, Vec<PageProblem>>>
}

/// Result type when polling `CrawlerStream`.
#[derive(serde::Serialize, Debug)]
pub struct CrawlerStreamResult {
    #[serde(serialize_with = "serialize_url_as_str")]
    pub url: Url,
    pub status_code: Option<u16>,
    /// Found links on a page which will be followed by the
    /// `CrawlerStream` if it was not parsed already.
    pub crawl_candidates: Vec<Url>,

    /// Problems found on the page
    pub problems: Vec<PageProblem>,

    #[serde(skip)]
    make_ctor_private: PhantomData<()>,
}

fn serialize_url_as_str<S>(input: &Url, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(input.as_str())
}

impl CrawlerStreamResult {
    pub fn is_ok(&self) -> bool {
        match self.status_code {
            Some(v) => v < 300 && v >= 200,
            None => false,
        }
    }
    fn new_error(url: Url, status_code: Option<u16>, problems: Vec<PageProblem>) -> Self {
        Self {
            url,
            status_code,
            crawl_candidates: Vec::new(),
            problems,
            make_ctor_private: PhantomData,
        }
    }
}

struct ParserResult {
    crawl_candidates: Vec<Url>,
    problems: Vec<PageProblem>,
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, serde::Serialize)]
pub enum PageProblem {
    #[non_exhaustive]
    InsecureLink {
        url: Url,
    },
    /// CrawlJob returned a non successful status code or failed to receive body
    NotOk,
    RedirectTarget {
        location: Option<String>,
    },
}

/// Streams `CrawlerStreamResult`s for successful and failed crawljobs
pub struct CrawlerStream<T: Fn(&Url) -> CrawlJob> {
    client: T,
    settings: CrawlerSettings,
    scheduled: std::collections::HashSet<url::Url>,
    running: Vec<(Url, CrawlJob)>,
}

impl<T: Fn(&Url) -> CrawlJob + Unpin> Stream for CrawlerStream<T> {
    type Item = CrawlerStreamResult;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.running.len() == 0 {
            log::trace!("Finished with urls count: '{:?}'", self.scheduled.len());
            return Poll::Ready(None);
        }

        let nr_of_concurrent_requests = self.settings.concurrent_requests;
        let item = self
            .running
            .iter_mut()
            .take(nr_of_concurrent_requests)
            .enumerate()
            .find_map(|(i, f)| match f.1.poll_unpin(cx) {
                Poll::Ready(e) => Some((i, e)),
                Poll::Pending => None,
            });

        if let Some((idx, result)) = item {
            self.running.swap_remove(idx).0;
            let links = &result.crawl_candidates;
            log::trace!(
                "Suggested links (total: {}): {:?},",
                links.len(),
                links.iter().take(10).map(|url| url.to_string())
            );
            self.schedule(links);
            return Poll::Ready(Some(result));
        }
        Poll::Pending
    }
}

/// Streams page information starting from a single URL
impl<T: Fn(&Url) -> CrawlJob> CrawlerStream<T> {
    pub fn new(client: T, start_url: Url) -> Self {
        Self::with_settings(client, start_url, CrawlerSettings::default())
    }
    pub fn with_settings(client: T, start_url: Url, settings: CrawlerSettings) -> Self {
        let future = (client)(&start_url);
        let mut scheduled = HashSet::new();
        scheduled.insert(start_url.clone());

        Self {
            client,
            settings,
            scheduled,
            running: vec![(start_url, future)],
        }
    }

    /// Schedules urls which are not processed yet until settings.page_limit is reached
    fn schedule(&mut self, links: &Vec<Url>) {
        log::trace!("Schedule '{}' items", links.len());
        for link in links.into_iter() {
            log::trace!("Check link: {}", link);
            if self.scheduled.len() >= self.settings.page_limit {
                break;
            }
            let mut link_without_hash = link.clone();
            link_without_hash.set_fragment(None);

            if !self.scheduled.insert(link_without_hash.clone()) {
                continue;
            }
            let task = (self.client)(&link_without_hash);
            self.running.push((link_without_hash, task));
        }
    }
}

pub struct CrawlerSettings {
    /// Number of pages to parse. This number includes pages which produced an error on crawling.
    /// This setting should prevent the user from creating infinitely many requests, as
    /// CrawlerStream has to maintain a HashSet of visited urls which could otherwise overflow the memory.
    /// use `future::Stream::take()` with a sufficient page_limit if you want an exact number of successful results.
    page_limit: usize,

    /// Maximal timeout until a request is cancelled. The crawler continues parsing other queued urls
    page_timeout: std::time::Duration,

    /// Number of concurrently open requests to the crawl target. IO is done on the awaiting thread,
    /// parsing uses the `actix_web::web::block()` threadpool. CrawlerStream doesn't spawn any new threads.
    concurrent_requests: usize,
}

impl Default for CrawlerSettings {
    fn default() -> Self {
        CrawlerSettings {
            page_limit: 5000,
            page_timeout: std::time::Duration::from_secs(1),
            concurrent_requests: 50,
        }
    }
}

fn parse(url: &Url, body: &Bytes) -> ParserResult {
    let str = String::from_utf8_lossy(body.bytes());
    let fragment = Html::parse_document(str.as_ref());
    let selector = Selector::parse("a[href]").unwrap();
    let interesting_links = fragment
        .select(&selector)
        .filter_map(|link_el| try_make_crawlable_url(url, link_el.value().attr("href").unwrap())
            .filter(|link |link.domain() == url.domain()));

    let mut crawl_candidates = Vec::new();
    let mut problems = Vec::new();

    for url in interesting_links {
        if url.scheme() == "http" {
            problems.push(PageProblem::InsecureLink { url });
        } else {
            crawl_candidates.push(url);
        }
    }
    ParserResult {
        crawl_candidates,
        problems,
    }
}

fn try_make_crawlable_url(base_url: &Url, link: &str) -> Option<Url> {
    let base = Url::options().base_url(Some(base_url));

    if let Ok(link) = base.parse(link) {
        if link.scheme().starts_with("http") {
            return Some(link);
        }
    }
    None
}

/// Factory to create CrawlerStreams from urls
pub struct CrawlerStreamFactory {
    client: actix_web::client::Client,
    settings: CrawlerSettings,
}

impl CrawlerStreamFactory {
    pub fn new_rustls() -> Self {
        let mut config = rustls::ClientConfig::new();
        config
            .root_store
            .add_server_trust_anchors(&webpki_roots::TLS_SERVER_ROOTS);
        let connector = actix_web::client::Connector::new()
            .rustls(Arc::new(config))
            .finish();
        let client = actix_web::client::Client::builder()
            .connector(connector)
            .finish();
        Self {
            client,
            settings: CrawlerSettings::default(),
        }
    }

    /// Creates a new CrawlerStream for the provided URL
    pub fn create(&self, start_url: Url) -> CrawlerStream<impl Fn(&Url) -> CrawlJob> {
        let client = self.client.clone();
        let timeout = self.settings.page_timeout;
        CrawlerStream::new(
            move |url| {
                let request = client.get(url.as_str()).timeout(timeout).send();

                let parse_url = url.clone();
                async move {
                    let mut response = match request.await {
                        Ok(r) => r,
                        Err(_) => return CrawlerStreamResult::new_error(parse_url, None, vec![]),
                    };
                    let a = HttpResponse {
                        request_url: parse_url.clone(),
                        headers: response.headers().into_iter().map(|(name, value)| (
                            name.as_str().to_string(), 
                            value.as_bytes().into()
                        )).collect(),
                        status: response.status().as_u16(),
                        body_future: async move { response.body().await.map_err(|x| {                            
                            if let PayloadError::Overflow = x {
                                Vec::new()
                            } else {
                                vec![PageProblem::NotOk]
                            }
                        })}.boxed_local()
                    };

                    parse_response(a).await
                }
                .boxed_local()
            },
            start_url,
        )
    }
}

async fn parse_response(response: HttpResponse) -> CrawlerStreamResult {
    if response.status >= 200 && response.status < 300 {
        parse_success_response(response).await
    } else if response.status >= 300 && response.status < 400 {
        parse_redirect_response(response)
    } else {
        parse_error_response(response)
    }
}

fn parse_redirect_response(response: HttpResponse) -> CrawlerStreamResult {
    let problem = match response.headers.get("location") {
        Some(location_header) => match String::from_utf8(location_header.clone()) {
            Ok(location) => match try_make_crawlable_url(&response.request_url, &location) {
                Some(redirect_url) => {
                    return CrawlerStreamResult {
                        url: response.request_url,
                        status_code: Some(response.status),
                        crawl_candidates: vec![redirect_url],
                        problems: Vec::new(),
                        make_ctor_private: PhantomData,
                    }
                }
                None => PageProblem::RedirectTarget {
                    location: Some(location.to_string()),
                },
            },
            Err(_) => PageProblem::RedirectTarget {
                location: Some(String::from_utf8_lossy(location_header).to_string()),
            },
        },
        None => PageProblem::RedirectTarget { location: None },
    };
    CrawlerStreamResult::new_error(response.request_url, Some(response.status), vec![problem])
}

fn parse_error_response(response: HttpResponse) -> CrawlerStreamResult {
    return CrawlerStreamResult::new_error(
        response.request_url,
        Some(response.status),
        vec![PageProblem::NotOk],
    );
}

async fn parse_success_response(response: HttpResponse) -> CrawlerStreamResult {
    let status_code = response.status;

    let body: Bytes = match response.body_future.await {
        Ok(r) => r,
        Err(e) => {
            return CrawlerStreamResult::new_error(
                response.request_url,
                Some(status_code),
                e,
            )
        }
    };
    let result_url = response.request_url.clone();
    let parse_result = actix_web::web::block::<_, _, ()>(move || Ok(parse(&result_url, &body)))
        .await
        .unwrap();

    CrawlerStreamResult {
        url: response.request_url,
        status_code: Some(status_code),
        crawl_candidates: parse_result.crawl_candidates,
        problems: parse_result.problems,
        make_ctor_private: PhantomData,
    }
}

#[cfg(test)]
mod tests {
    use actix_web::web::{Buf, BufMut, BytesMut};
    use {super::*, std::collections::HashMap};

    #[actix_rt::test]
    async fn resolve_recursive_once() {
        let home_url = Url::parse("https://foo.ch").unwrap();
        let home_with_slash = Url::parse("https://foo.ch/").unwrap();
        let home_with_hashtag = Url::parse("https://foo.ch/#").unwrap();
        let bar_url = Url::parse("https://foo.ch/bar").unwrap();
        let mut map = HashMap::new();
        map.insert(home_url.clone(), vec![bar_url.clone()]);
        map.insert(home_with_slash.clone(), vec![bar_url.clone()]);
        map.insert(home_with_hashtag.clone(), vec![bar_url.clone()]);
        map.insert(bar_url.clone(), vec![home_url.clone(), home_with_hashtag, home_with_slash]);

        let crawler = CrawlerStream::new(
            move |url| {
                let result = CrawlerStreamResult {
                    url: url.clone(),
                    status_code: Some(200),
                    crawl_candidates: map.get(url).unwrap().clone(),
                    problems: Vec::new(),
                    make_ctor_private: PhantomData,
                };
                async move { result }.boxed_local()
            },
            home_url,
        );

        let all = crawler.collect::<Vec<_>>().await;

        assert_eq!(2, all.len());
    }

    #[test]
    fn parse_supports_linktypes() {
        for source in [
            "https://mineichen.ch/home/next.php",
            "/home/next.php",
            "next.php",
        ]
        .iter()
        {
            let source_url = Url::parse("https://mineichen.ch/home/current.php?test=1").unwrap();
            let expected_url = Url::parse("https://mineichen.ch/home/next.php").unwrap();
            let content = build_page_with_links(std::iter::once(*source));
            let mut results = parse(&source_url, &content).crawl_candidates.into_iter();
            assert_eq!(Some(expected_url), results.next());
            assert_eq!(None, results.next());
        }
    }

    #[test]
    fn parse_doesnt_return_invalid_links() {
        for source in ["https://mineichen.ch.com/", "ftp://mineichen.ch"].iter() {
            let source_url = Url::parse("https://mineichen.ch/home/current.php?test=1").unwrap();
            let content = build_page_with_links(std::iter::once(*source));
            let mut results = parse(&source_url, &content).crawl_candidates.into_iter();
            assert_eq!(None, results.next());
        }
    }

    #[test]
    fn parse_adds_problem_for_http_link() {
        let valid1 = "https://mineichen.ch/home/previous.php";
        let http = "http://mineichen.ch/home/insecure.php";
        let valid2 = "https://mineichen.ch/home/next.php";

        let source_url = Url::parse("https://mineichen.ch/").unwrap();
        let content = build_page_with_links(vec![valid1, http, valid2]);
        let result = parse(&source_url, &content);
        assert_eq!(2, result.crawl_candidates.len());

        assert_eq!(
            vec!(PageProblem::InsecureLink {
                url: Url::parse(http).unwrap()
            }),
            result.problems
        );
    }
    #[test]
    fn schedule_accepts_urls_after_already_present() {
        let home = Url::parse("https://example.ch").unwrap();
        let mut stream = build_dummy_stream(home.clone());

        stream.schedule(&vec![home, Url::parse("https://example.ch/foo").unwrap()]);
        assert_eq!(2, stream.scheduled.len());
        assert_eq!(2, stream.running.len());
    }
    #[actix_rt::test]
    async fn schedule_stops_after_limit() {
        let home = Url::parse("https://example.ch").unwrap();
        let mut stream = build_dummy_stream(home.clone());

        stream.schedule(
            &(0..20)
                .map(|x| Url::parse(&format!("https://google.ch/{}", x)).unwrap())
                .collect(),
        );
        assert_eq!(10, stream.scheduled.len());
        assert_eq!(10, stream.running.len());

        stream.next().await;
        assert_eq!(10, stream.scheduled.len());
        assert_eq!(9, stream.running.len());
        stream.schedule(&vec![Url::parse("https://google.ch/other").unwrap()]);

        assert_eq!(10, stream.scheduled.len());
        assert_eq!(9, stream.running.len());
    }

    fn build_dummy_stream(home: Url) -> CrawlerStream<impl Fn(&Url) -> CrawlJob> {
        CrawlerStream::with_settings(
            move |url| {
                let result = CrawlerStreamResult::new_error(url.clone(), Some(200), Vec::new());
                async move { result }.boxed_local()
            },
            home.clone(),
            CrawlerSettings {
                page_limit: 10,
                ..CrawlerSettings::default()
            },
        )
    }

    fn build_page_with_links(links: impl IntoIterator<Item = &'static str>) -> Bytes {
        let mut data = BytesMut::new();
        data.put(&b"<html><head></head><body>"[..]);
        for link in links {
            data.put(&b"<a href=\""[..]);
            data.put(link.as_bytes());
            data.put(&b"\">Link</a>"[..]);
        }

        data.put(&b"</body></html>"[..]);
        data.to_bytes()
    }
}
