FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY services/build-service/ ./services/build-service/
RUN go build -o build-service ./services/build-service/

FROM alpine:3.23
WORKDIR /app
COPY --from=builder /app/build-service .
CMD ["./build-service"]
