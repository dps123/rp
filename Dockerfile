FROM golang:alpine as build
RUN mkdir -p $GOPATH/src/github.com/dps123/rp
WORKDIR $GOPATH/src/github.com/dps123/rp
ADD . .

RUN go build -o /app/rp

FROM alpine
RUN apk add --no-cache ca-certificates
COPY --from=build /app/rp /usr/local/bin/rp
