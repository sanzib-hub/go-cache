run: build
	@./bin/go-redis

build:
	@go build -o bin/go-redis
