GOOS=linux
GOARCH=amd64
BUILD_OPTS=-trimpath -tags timetzdata

all:
	go build $(BUILD_OPTS) -o gmqcli.bin gmq/cmd/gmqcli/main.go
