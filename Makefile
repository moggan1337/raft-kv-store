.PHONY: build test lint run clean

GO ?= go
BIN := bin/raftkvd

build:
	mkdir -p bin
	$(GO) build -o $(BIN) ./cmd/raftkvd

test:
	$(GO) test -race -count=1 ./...

lint:
	golangci-lint run ./...

run: build
	./$(BIN)

clean:
	rm -rf bin
