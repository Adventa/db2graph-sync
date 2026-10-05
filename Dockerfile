# Standalone db2graph-sync image.
# db2graph-sync — an open-source project by Adventa. https://www.adventa.com.au
#
# Status: NOT YET BUILT OR SCANNED in this release (no container runtime was
# available). Pin base images by digest before publishing (see RELEASING.md).
#
#   docker build -t db2graph-sync:dev .
#   docker run --rm -v ./config.toml:/etc/db2graph-sync/config.toml:ro \
#     -v d2g-state:/var/lib/db2graph-sync db2graph-sync:dev run -c /etc/db2graph-sync/config.toml

FROM rust:1.90-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p db2graph-sync-cli

FROM gcr.io/distroless/cc-debian12:nonroot
LABEL org.opencontainers.image.title="db2graph-sync" \
      org.opencontainers.image.description="One-way CDC projection from operational databases into graph databases and vector indexes. An open-source project by Adventa." \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.vendor="Adventa" \
      org.opencontainers.image.url="https://www.adventa.com.au"
COPY --from=build /src/target/release/db2graph-sync /usr/local/bin/db2graph-sync
COPY LICENSE NOTICE THIRD_PARTY_NOTICES.md /usr/share/doc/db2graph-sync/
# State must live on a persistent volume owned by the nonroot user (65532).
VOLUME ["/var/lib/db2graph-sync"]
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/db2graph-sync"]
CMD ["--help"]
