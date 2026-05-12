FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY services/sandbox-orchestrator/ ./services/sandbox-orchestrator/
RUN go build -o sandbox-orchestrator ./services/sandbox-orchestrator/

FROM alpine:3.23
WORKDIR /app
COPY --from=builder /app/sandbox-orchestrator .
CMD ["./sandbox-orchestrator"]
