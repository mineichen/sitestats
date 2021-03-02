# Sitestat
Sitestat aims to be a tool to analyze references within a website and helps to find dead links and insecure http-references to pages in the same domain. It currently supports https pages only.

## Features
- [x] List reachable links
- [x] Count reachable links
- [ ] Settings from cli arguments (Currently crawling is limited to max. 5000 pages/domain)
- [ ] Automatically follow redirects when crawling
- [ ] List broken links
- [ ] Error-details for failed requests
- [ ] Cache requests

## Usage
- http get /crawler/domain/{searchdomain}
  - Response (http 200) - Stream of all valid urls found on the searchdomain separated by newline
  - Response (http 400) - Format in body is yet unstable
- http get /crawler/domain/{searchdomain}/count
  - Response (http 200) - Body with a single number representing number of reachable links.
  - Response (http 400) - Format in body is yet unstable


### Build and save the image
```
docker build -t sitestats:0.0.x .
docker save sitestats:0.0.x | gzip > release-0.0.x.tar.gz
```

### Load and run
```
docker load -i release-0.0.x.tar.gz
docker run --rm -p 8080:8080 sitestats:0.0.x
```


# Technical Details
The tool uses the [actix-web](https://docs.rs/actix-web/3.3.2/actix_web/index.html) framework. To crawl the websites, the builtin Client is used to fetch all pages asynchronously and parallel within a single thread. To avoid blocking the actix-thread, the dom parsing with [scraper](https://docs.rs/scraper/0.12.0/scraper/) done in the [actix_web::web::block](https://actix.rs/actix-web/actix_web/web/fn.block.html) threadpool.

## Leightweight container with MUSL
When compiling and statically linking against linux-musl, the resulting binary doesn't have any dependencies to shared libraries and can therefore be deployed directly onto the empty 'scratch' docker image. This results in a lightweight 16.4MB image which includes just the bare minimum and thus has a very small attack surface and doesn't requiring frequent os updates.
