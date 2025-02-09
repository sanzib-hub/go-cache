run: build
	@./bin/go-redis --listenAddr :8976

build:
	@go build -o bin/go-redis
