.PHONY: install tidy fmt

install: fmt
	go install .

tidy:
	go mod tidy
	
fmt:
	go fmt ./...
