ARG BUILDER_IMAGE=golang:1.25
ARG BASE_IMAGE=gcr.io/distroless/static:nonroot

# Build the sidecar binary
FROM --platform=${BUILDPLATFORM} ${BUILDER_IMAGE} AS builder
ARG TARGETOS
ARG TARGETARCH
ARG CGO_ENABLED=0

WORKDIR /workspace
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building so that we don't need to re-download as much
RUN go mod download

COPY api/ api/
COPY pkg/ pkg/
COPY cmd/controller/ cmd/controller/
COPY client-go/ client-go/

# Build
RUN CGO_ENABLED=${CGO_ENABLED} GOOS=linux GOARCH=${TARGETARCH} go build -a -o sidecar cmd/controller/main.go

FROM --platform=${BUILDPLATFORM} ${BASE_IMAGE}
WORKDIR /
COPY --from=builder /workspace/sidecar .
USER 65532:65532

ENTRYPOINT ["/sidecar"]
