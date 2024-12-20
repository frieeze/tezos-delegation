build: 
	@go build -o bin/tds cmd/tds/main.go

run:
	@./bin/tds

dev:
	@go run cmd/tds/main.go -debug

test:
	@go test -coverprofile /tmp/tds-go-coverage -timeout 10s -v ./...

empty:
	@go run cmd/db/main.go -empty

sync: 
	@go run cmd/db/main.go
