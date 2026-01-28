.PHONY: run bench download build

run:
	go run ./cmd/postings

bench:
	go run ./cmd/bench

download:
	go run ./cmd/bench download

build:
	go build -o postings ./cmd/postings
	go build -o bench ./cmd/bench

verify:
	go run ./cmd/verify

test:
	go test ./...