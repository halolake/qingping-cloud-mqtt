FROM golang:1.15-alpine AS builder
WORKDIR /go/src/github.com/halozheng/qingping-cloud-mqtt
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o app .

FROM alpine:latest
COPY --from=builder /go/src/github.com/halozheng/qingping-cloud-mqtt/app /
CMD ["/app"]
