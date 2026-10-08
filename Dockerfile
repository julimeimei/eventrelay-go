ARG GO_VERSION=1.26

FROM golang:${GO_VERSION}-alpine AS builder

WORKDIR /src

RUN apk add --no-cache ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG COMMAND=api
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/eventrelay ./cmd/${COMMAND}

FROM alpine:3.22

RUN apk add --no-cache ca-certificates wget \
    && addgroup -S eventrelay \
    && adduser -S -G eventrelay eventrelay

WORKDIR /app

COPY --from=builder /out/eventrelay /usr/local/bin/eventrelay
COPY --from=builder /src/migrations ./migrations

USER eventrelay

ENTRYPOINT ["/usr/local/bin/eventrelay"]
