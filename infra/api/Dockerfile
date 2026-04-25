FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o api ./cmd/api

FROM alpine:3.19
WORKDIR /api
COPY --from=builder /app/api .
EXPOSE 8080
ENTRYPOINT ["./api"]