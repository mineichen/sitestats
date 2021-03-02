ARG PROJECT_NAME=spec-your-project

FROM ekidd/rust-musl-builder:1.50.0 AS build
USER root
RUN mkdir -p /usr/rust/src/app/target && chown -R rust /usr/rust/src/app/target
USER rust
WORKDIR /usr/rust/src/app
COPY ./src/ src
COPY Cargo.toml Cargo.lock ./

RUN cargo build --release

# Copy the statically-linked binary into a scratch container.
FROM scratch
ARG PROJECT_NAME
COPY --from=build /usr/rust/src/app/target/x86_64-unknown-linux-musl/release/sitestats /app
ENV RUST_BACKTRACE 1
USER 1000
CMD ["./app"]