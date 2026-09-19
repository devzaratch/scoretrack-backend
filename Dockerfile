FROM golang:alpine AS builder
WORKDIR /app
ENV GOTOOLCHAIN=auto
RUN apk --no-cache add git ca-certificates
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o go-backend .

FROM alpine:latest
WORKDIR /app
RUN apk --no-cache add ca-certificates tzdata
COPY --from=builder /app/go-backend .
VOLUME ["/app/data"]
EXPOSE 8080
ENV PORT=8080
CMD ["./go-backend"]
