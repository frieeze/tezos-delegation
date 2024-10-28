build: 
	@go build -o bin/$(tds) cmd/$(tds)/main.go

run:
	@go run cmd/$(tds)/main.go

test:
	@go test -coverprofile /tmp/tds-go-coverage -v ./...

