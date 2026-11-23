# Build stage
FROM golang:1.27-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o scoretrack-backend .

# Run stage
FROM alpine:3.22
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/scoretrack-backend .

# Default port
EXPOSE 8080

CMD ["./scoretrack-backend"]
