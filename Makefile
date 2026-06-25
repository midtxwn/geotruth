.PHONY: build test vet tidy clean

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

tidy:
	go mod tidy

clean:
	go clean