FROM rust:1-slim AS rust-builder

RUN apt-get update && apt-get install -y --no-install-recommends build-essential pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY rust_search/Cargo.toml rust_search/Cargo.lock ./rust_search/
COPY rust_search/src ./rust_search/src
COPY index.html results.html dashboard.html universities.json ./

RUN cargo build --locked --release --manifest-path rust_search/Cargo.toml

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=rust-builder /app/rust_search/target/release/velo-search-rs /usr/local/bin/velo-search-rs
COPY index.html results.html dashboard.html universities.json ./

ENV SEARCH_BACKEND=tidb
ENV PORT=7860
ENV VELO_PROJECT_ROOT=/app
EXPOSE 7860

CMD ["/usr/local/bin/velo-search-rs"]
