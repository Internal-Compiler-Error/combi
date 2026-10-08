# Development image for the bulk crawler: Rust toolchain + sqlx-cli + psql.
# The source tree is bind-mounted at /workspace by docker-compose.yml, so edits on
# the host are picked up without rebuilding the image.
FROM rust:1.97-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client \
    && rm -rf /var/lib/apt/lists/*

RUN rustup component add rustfmt clippy

# sqlx-cli runs and creates migrations
RUN cargo install sqlx-cli --locked --no-default-features --features rustls,postgres

WORKDIR /workspace

CMD ["bash"]
