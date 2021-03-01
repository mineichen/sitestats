//! Lacking of a good/maintained crate alternative. 
//! - Quick_crawler:0.1.2: the best candidate found, still very young ( during evaluation), no recursion support, aborts on error
//! - url-crawler:0.3.0: uses threads. While usable for a few users, it requires too many OS threads to be used within a web api.                     

use std::{collections::HashSet, marker::PhantomData, pin::Pin, sync::Arc, task::{Context, Poll}};
use futures::{prelude::*, Stream, future::LocalBoxFuture};
use actix_web::{client::{Client, Connector}, web::{Buf, Bytes}};
use scraper::{Html, Selector};
use url::Url;

// Factory to create new jobs for the crawler.
trait CrawlJobFactory: Send + Sync + 'static {
    fn call<'a>(&'a self, url: Url) -> CrawlJob;
}

impl<F: Send + Sync + 'static, Fut> CrawlJobFactory for F
where
    F: Fn(Url) -> Fut,
    Fut: Future<Output=CrawlJobResult> + Send + 'static,
{
    fn call<'a>(&'a self, url: Url) -> CrawlJob {
        ((self)(url)).boxed_local()     
    }
}

type CrawlJobResult = Result<Vec<Url>, Error>;
type CrawlJob = LocalBoxFuture<'static, CrawlJobResult>;

/// Result type when polling `CrawlerStream`
pub struct CrawlerStreamResult {
    pub url: Url,
    pub result: Result<Success, Error>,
    make_ctor_private: PhantomData<()>
}

impl CrawlerStreamResult {
    fn new_ok(url: Url, links: Vec<Url>) -> Self {
        Self {
            url,
            result: Ok(Success { links }),
            make_ctor_private: PhantomData
        }
    }
    fn new_error(url: Url) -> Self {
        Self {
            url,
            result: Err(Error { }),
            make_ctor_private: PhantomData
        }
    }
}
/// Success type of `CrawlerStreamResult`
pub struct Success {
    pub links: Vec<Url>
}

/// Error type of `CrawlerStreamResult`
#[derive(Debug)]
pub struct Error {
    
}

/// Streams `CrawlerStreamResult`s for successful and failed crawljobs
pub struct CrawlerStream<T: Fn(&Url) -> CrawlJob> {
    client: T,
    settings: CrawlerSettings,
    scheduled: std::collections::HashSet<url::Url>,
    running: Vec<(Url, CrawlJob)>
}

impl<T: Fn(&Url) -> CrawlJob + Unpin> Stream for CrawlerStream<T> {
    type Item = CrawlerStreamResult;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.running.len() == 0 {
            log::trace!("Finished with urls count: '{:?}'", self.scheduled.len());
            return Poll::Ready(None);
        }
        
        let concurrent = self.settings.concurrent_requests;
        let item = self.running.iter_mut().take(concurrent).enumerate().find_map(|(i, f)| {
            match f.1.poll_unpin(cx) {
                Poll::Pending => None,
                Poll::Ready(e) => Some((i, e)),
            } 
        });
        
        if let Some((idx, result)) = item { 
            let url = self.running.swap_remove(idx).0;
            if let Ok(links) = result {
                log::trace!(
                    "Suggested links (total: {}): {:?}",  
                    links.len(),
                    links.iter().take(10).map(|url| url.to_string()));
                self.schedule(&links);
                return Poll::Ready(Some(CrawlerStreamResult::new_ok(url, links)))
            } else {
                log::trace!(
                    "Error in url: {}",
                    url
                );
                return Poll::Ready(Some(CrawlerStreamResult::new_error(url)));
            }
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
            running: vec![(start_url, future)]
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

    /// Maximal timeout until a request is cancelled. The crawler still continues parsing and returns an Error
    page_timeout: std::time::Duration,

    /// Number of concurrently open requests to the crawl target. IO is done on the awaiting thread,
    /// parsing uses the `actix_web::web::block()` threadpool. CrawlerStream doesn't spawn any new threads.
    concurrent_requests: usize
}

impl Default for CrawlerSettings {
    fn default() -> Self {
        CrawlerSettings {
            page_limit: 1000,
            page_timeout: std::time::Duration::from_secs(1),
            concurrent_requests: 20
        }
    }
}

fn parse(url: &Url, data: Bytes) -> Result<Vec<Url>, Error> {
    let str = String::from_utf8_lossy(data.bytes());
    let fragment = Html::parse_document(str.as_ref());
    let selector = Selector::parse("a[href]").unwrap();
    let links: Vec<Url> = fragment.select(&selector)
        .filter_map(|link_el| {   
            let base = Url::options().base_url(Some(url));
            base.parse(link_el.value().attr("href").unwrap()).ok()            
        })
        .filter(|a| a.domain() == url.domain() && a.scheme().starts_with("http"))
        .collect();

    Ok(links)
}

/// Factory to create CrawlerStreams from urls
pub struct CrawlerStreamFactory {
    client: Client,
    settings: CrawlerSettings
}

impl CrawlerStreamFactory {
    pub fn new_rustls() -> Self {
        let mut config = rustls::ClientConfig::new();
        config.root_store.add_server_trust_anchors(&webpki_roots::TLS_SERVER_ROOTS);
        let client = Client::builder()
            .connector(Connector::new().rustls(Arc::new(config)).finish())
            .finish();
        Self { client, settings: CrawlerSettings::default() }
    }

    /// Creates a new CrawlerStream for the provided URL 
    pub fn create(&self, start_url: Url) -> CrawlerStream<impl Fn(&Url) -> CrawlJob> {
        let client = self.client.clone();
        let timeout = self.settings.page_timeout;
        CrawlerStream::new(
            move |url| {
                let request = client
                    .get(url.as_str())
                    .timeout(timeout)
                    .send();

                let parse_url = url.clone();
                async move {
                    let mut response = request.await.map_err(|_| Error { })?;
                    if response.status() != 200 {
                        log::info!("Redirect not supportedCode: {}, Url: {}", response.status(), parse_url.as_str());
                        return Err(Error {})
                    }
                    let body = response.body().await.map_err(|_| Error { })?;
                    let vec = actix_web::web::block(move || { parse(&parse_url, body) }).await
                        .map_err(|_| Error { })?;
                    Ok(vec)
                }.boxed_local()
            }, 
            start_url)
    }
}

#[cfg(test)]
mod tests {
    use actix_web::web::{Buf, BufMut, BytesMut};
    use {super::*, std::collections::HashMap};
    
    #[test]
    fn resolve_recursive_once() {
        
        let home_url = Url::parse("https://foo.ch").unwrap();
        let home_with_slash = Url::parse("https://foo.ch/").unwrap();
        let bar_url = Url::parse("https://foo.ch/bar").unwrap();
        let mut map = HashMap::new();
        map.insert(home_url.clone(), vec![bar_url.clone()]);
        map.insert(home_with_slash, vec![bar_url.clone()]);
        map.insert(bar_url.clone(), vec![home_url.clone()]);

        let crawler = CrawlerStream::new(
            move |url| {
                let result = map.get(url).unwrap().clone();
                async move { 
                    Ok(result) 
                }.boxed_local()
            }, 
            home_url);

        let all = futures::executor::block_on(async {                
            crawler.collect::<Vec<_>>().await
        });

        assert_eq!(2, all.len());
    }

    #[test]
    fn parse_supports_linktypes() {
        for source in ["https://mineichen.ch/home/next.php", "/home/next.php", "next.php"].iter() {
            let source_url = Url::parse("https://mineichen.ch/home/current.php?test=1").unwrap();
            let expected_url = Url::parse("https://mineichen.ch/home/next.php").unwrap();
            let content = build_page_with_links(std::iter::once(*source));
            let mut results = parse(&source_url, content).expect("Expected parse to succeed").into_iter();
            assert_eq!(Some(expected_url), results.next());
            assert_eq!(None, results.next());
        }
    }

    #[test]
    fn parse_doesnt_return_invalid_links() {
        for source in ["https://mineichen.ch.com/", "ftp://mineichen.ch"].iter() {
            let source_url = Url::parse("https://mineichen.ch/home/current.php?test=1").unwrap();
            let content = build_page_with_links(std::iter::once(*source));
            let mut results = parse(&source_url, content).expect("Expected parse to succeed").into_iter();
            assert_eq!(None, results.next());
        }       
    }

    fn build_page_with_links(links: impl IntoIterator<Item=&'static str>) -> Bytes {
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
