# syntax=docker/dockerfile:1
FROM gcr.io/distroless/base-nossl-debian13@sha256:af5cb8dd589b8520b8c06bebb9efb73d7e16406cab58e85c51761fff49d370a0
COPY --chmod=0555 bin/controller-linux /controller
USER 65532:65532
ENTRYPOINT ["/controller"]
