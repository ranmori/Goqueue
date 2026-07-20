# Build stage
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /goqueue .

# Runtime stage — just the binary, no Go toolchain, small attack surface
FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /goqueue /goqueue
EXPOSE 8080
ENTRYPOINT ["/goqueue"]
