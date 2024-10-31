build: 
	@go build -o bin/tds cmd/tds/main.go

run:
	@./bin/tds

dev:
	@go run cmd/tds/main.go -dev -debug

test:
	@go test -coverprofile /tmp/tds-go-coverage -v ./...

empty:
	@go run cmd/db/main.go -empty

sync: 
	@go run cmd/db/main.go
