# syntax=docker/dockerfile:1
FROM gcr.io/distroless/base-nossl-debian13@sha256:3a2dbf10c2fdafe5865e2b03a4a3b9b23f2e280229a81730415379b76dc70592
COPY --chmod=0555 bin/controller-linux /controller
USER 65532:65532
ENTRYPOINT ["/controller"]
