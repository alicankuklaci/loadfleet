# syntax=docker/dockerfile:1
# Expects prebuilt static binaries in dist/linux-<arch>/loadfleet (scripts/build-dist.sh linux).
FROM --platform=$BUILDPLATFORM alpine:3.20 AS base
ARG TARGETARCH
RUN apk add --no-cache ca-certificates && mkdir -p /out/data
# --chmod: CI artifact upload/download drops the exec bit
COPY --chmod=755 dist/linux-${TARGETARCH}/loadfleet /out/loadfleet

FROM scratch
COPY --from=base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=base --chown=65534:65534 /out/data /data
COPY --from=base /out/loadfleet /loadfleet
COPY LICENSE THIRD_PARTY_NOTICES /
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
WORKDIR /data
USER 65534:65534
EXPOSE 8080
ENTRYPOINT ["/loadfleet"]
CMD ["--help"]
