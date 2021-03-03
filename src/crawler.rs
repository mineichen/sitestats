//! Lacking of a good/maintained crate alternative.
//! - Quick_crawler:0.1.2: the best candidate found, still very young ( during evaluation), no recursion support, aborts on error
//! - url-crawler:0.3.0: uses threads. While usable for a few users, it requires too many OS threads to be used within a web api.                     

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

// Factory to create new jobs for the crawler.
trait CrawlJobFactory: Send + Sync + 'static {
    fn call<'a>(&'a self, url: Url) -> CrawlJob;
}

impl<F: Send + Sync + 'static, Fut> CrawlJobFactory for F
where
    F: Fn(Url) -> Fut,
    Fut: Future<Output = CrawlerStreamResult> + Send + 'static,
{
    fn call<'a>(&'a self, url: Url) -> CrawlJob {
        ((self)(url)).boxed_local()
    }
}

type CrawlJob = LocalBoxFuture<'static, CrawlerStreamResult>;

/// Result type when polling `CrawlerStream`.
#[derive(serde::Serialize)]
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

/// Possible weaknesses detected within a page.
/// There will be further P
#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, serde::Serialize)]
pub enum PageProblem {
    #[non_exhaustive]
    InsecureLink { url: Url },
    /// CrawlJob returned a non successful status code or failed to receive body
    NotOk,
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

        let concurrent = self.settings.concurrent_requests;
        let item = self
            .running
            .iter_mut()
            .take(concurrent)
            .enumerate()
            .find_map(|(i, f)| match f.1.poll_unpin(cx) {
                Poll::Pending => None,
                Poll::Ready(e) => Some((i, e)),
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

    /// Schedules urls which are not scheduled yet until settings.page_limit is reached
    fn schedule(&mut self, links: &Vec<Url>) {
        log::trace!("Schedule '{}' items", links.len());
        for link in links.into_iter() {
            log::trace!("Check link: {}", link);
            if self.scheduled.len() >= self.settings.page_limit {
                break;
            }
            if !self.scheduled.insert(link.clone()) {
                continue;
            }
            let task = (self.client)(&link);
            self.running.push((link.clone(), task));
        }
    }
}

pub struct CrawlerSettings {
    /// Number of pages to parse. This number includes pages which produced an error on crawling.
    /// This setting should prevent the user from creating infinitely many requests, as
    /// CrawlerStream has to maintain a HashSet of visited urls which could otherwise overflow the memory.
    /// use `future::Stream::take()` with a sufficient page_limit if you want an exact number of successful results.
    page_limit: usize,

    /// Maximal timeout until a request is cancelled. The crawler still continues parsing
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
        .filter_map(|link_el| {
            let base = Url::options().base_url(Some(url));
            base.parse(link_el.value().attr("href").unwrap()).ok()
        })
        .filter(|a| a.domain() == url.domain() && a.scheme().starts_with("http"));

    let mut effectual_candidates = Vec::new();
    let mut problems = Vec::new();

    for url in interesting_links {
        if url.scheme() == "http" {
            problems.push(PageProblem::InsecureLink { url });
        } else {
            effectual_candidates.push(url);
        }
    }
    ParserResult {
        crawl_candidates: effectual_candidates,
        problems,
    }
}

/// Factory to create CrawlerStreams from urls
pub struct CrawlerStreamFactory {
    client: actix_web::client::Client,
    settings: CrawlerSettings,
}

type ActixClientResonse =
    actix_web::client::ClientResponse<actix_web::dev::Decompress<actix_web::dev::Payload>>;

impl CrawlerStreamFactory {
    pub fn new_rustls() -> Self {
        let mut config = rustls::ClientConfig::new();
        config
            .root_store
            .add_server_trust_anchors(&webpki_roots::TLS_SERVER_ROOTS);
        let client = actix_web::client::Client::builder()
            .connector(
                actix_web::client::Connector::new()
                    .rustls(Arc::new(config))
                    .finish(),
            )
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
                    let response = match request.await {
                        Ok(r) => r,
                        Err(_) => return CrawlerStreamResult::new_error(parse_url, None, vec![]),
                    };
                    let status_code = response.status().as_u16();
                    if !response.status().is_success() && !response.status().is_redirection() {
                        return CrawlerStreamResult::new_error(
                            parse_url,
                            Some(status_code),
                            vec![PageProblem::NotOk],
                        );
                    }

                    Self::parse_body(response, parse_url).await
                }
                .boxed_local()
            },
            start_url,
        )
    }
    async fn parse_body(mut response: ActixClientResonse, parse_url: Url) -> CrawlerStreamResult {
        let status_code = response.status().as_u16();
        let body = match response.body().await {
            Ok(r) => r,
            Err(e) => {
                return CrawlerStreamResult::new_error(
                    parse_url,
                    Some(status_code),
                    if let PayloadError::Overflow = e {
                        Vec::new()
                    } else {
                        vec![PageProblem::NotOk]
                    },
                )
            }
        };
        let result_url = parse_url.clone();
        let parse_result = actix_web::web::block::<_, _, ()>(move || Ok(parse(&parse_url, &body)))
            .await
            .unwrap();
        CrawlerStreamResult {
            url: result_url,
            status_code: Some(status_code),
            crawl_candidates: parse_result.crawl_candidates,
            problems: parse_result.problems,
            make_ctor_private: PhantomData,
        }
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
        let bar_url = Url::parse("https://foo.ch/bar").unwrap();
        let mut map = HashMap::new();
        map.insert(home_url.clone(), vec![bar_url.clone()]);
        map.insert(home_with_slash, vec![bar_url.clone()]);
        map.insert(bar_url.clone(), vec![home_url.clone()]);

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
