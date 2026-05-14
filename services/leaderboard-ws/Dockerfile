FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY services/leaderboard-ws/ ./services/leaderboard-ws/
RUN go build -o leaderboard-ws ./services/leaderboard-ws/

FROM alpine:3.23
WORKDIR /app
COPY --from=builder /app/leaderboard-ws .
CMD ["./leaderboard-ws"]
