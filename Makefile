build: 
	@go build -o bin/tds cmd/tds/main.go

run :
	@go run cmd/tds/main.go

dev:
	@go run cmd/tds/main.go -dev -debug

test:
	@go test -coverprofile /tmp/tds-go-coverage -v ./...

empty:
	@echo "Empty Storage"
	@go run cmd/db/main.go -empty

sync: 
	@echo "Sync Storage"
	@go run cmd/db/main.go
