FROM golang:1.17-alpine
WORKDIR /app
# This path must exist as it is used as a mount point for testing
# Ensure your app is loading files from this location
RUN mkdir /app/test-files

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY pkg ./pkg

RUN go install github.com/mailru/easyjson/...@latest
RUN easyjson -all pkg/record
RUN go build -o task-server cmd/main.go

CMD [ "./task-server" ]