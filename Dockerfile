FROM golang:1.25 AS build

ARG CMD=./cmd/gateway

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/app ${CMD}

FROM gcr.io/distroless/static:nonroot

COPY --from=build /out/app /app

USER nonroot:nonroot
ENTRYPOINT ["/app"]
