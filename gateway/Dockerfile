FROM golang:1.22-alpine AS builder

WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . ./

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /bin/gateway ./main.go


FROM alpine:3.19

RUN addgroup -S app && adduser -S -G app app \
&& mkdir -p /app/static \
&& chown -R app:app /app

WORKDIR /app

COPY --from=builder /bin/gateway /app/gateway

USER app

EXPOSE 8080

CMD ["/app/gateway"]