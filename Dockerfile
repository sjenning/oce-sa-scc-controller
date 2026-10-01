FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ ./cmd/
COPY internal/ ./internal/
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /controller ./cmd/oce-sa-scc-controller

FROM gcr.io/distroless/static:nonroot
COPY --from=build /controller /controller
USER 65532:65532
ENTRYPOINT ["/controller"]
