GOOS=linux
GOARCH=amd64
BUILD_OPTS=-trimpath -tags timetzdata
LDFLAGS="-X main.buildts=`date -u +%y%m%d_%H%m%S_%Z`"

all:
	go build $(BUILD_OPTS) -ldflags $(LDFLAGS) -o gmqcli.bin gmq/cmd/gmqcli/main.go
