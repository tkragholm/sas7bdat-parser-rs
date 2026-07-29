# A lab for measuring the converter against storage that behaves like a network share.
# See test-lab/README.md for what it does and does not reproduce.
FROM rustlang/rust:nightly-bookworm AS build
WORKDIR /src
# rust-toolchain.toml pins the nightly; rustup fetches it on the first build.
COPY rust-toolchain.toml ./
RUN rustup show
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release -p sas7bdat-cli --bin sas7bdat --no-default-features --features cli

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends fuse libfuse2 python3 python3-pip procps \
    && pip3 install --no-cache-dir --break-system-packages fusepy \
    && rm -rf /var/lib/apt/lists/*
# allow_other lets the mount be read by a different user than the one serving it
RUN echo user_allow_other >> /etc/fuse.conf

COPY --from=build /src/target/release/sas7bdat /usr/local/bin/sas7bdat
COPY test-lab/slowfs.py /usr/local/bin/slowfs.py
COPY test-lab/run.sh /usr/local/bin/run.sh
COPY scripts/probe.py /usr/local/bin/probe.py
RUN chmod +x /usr/local/bin/run.sh /usr/local/bin/slowfs.py

WORKDIR /mnt
ENTRYPOINT ["/usr/local/bin/run.sh"]
CMD ["bash"]
