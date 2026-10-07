# Tools image: sqlx-cli runs and creates migrations, psql for poking at the database.
# The source tree is bind-mounted at /workspace by docker-compose.yml.
FROM rust:1.97-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client \
    && rm -rf /var/lib/apt/lists/*

RUN cargo install sqlx-cli --locked --no-default-features --features rustls,postgres

WORKDIR /workspace

CMD ["bash"]
