FROM golang:1.22-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /logagent ./cmd/logagent

FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /logagent /usr/local/bin/logagent
EXPOSE 4080
ENTRYPOINT ["logagent"]
