# Development image: Rust toolchain + sqlx-cli + watchexec + psql.
# The source tree is bind-mounted at /workspace by docker-compose.yml, so edits on
# the host are picked up without rebuilding the image.
FROM rust:1.97-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client \
    && rm -rf /var/lib/apt/lists/*

RUN rustup component add rustfmt clippy

# sqlx-cli runs migrations and refreshes the offline query data in .sqlx/;
# watchexec restarts the API server when Rust sources change.
RUN cargo install sqlx-cli --locked --no-default-features --features rustls,postgres \
    && cargo install watchexec-cli --locked

WORKDIR /workspace

CMD ["bash"]
