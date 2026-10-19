GOOS?=linux
GOARCH?=amd64
CGO_ENABLED?=0

BUILD_OPTS=-ldflags="-s -w" -trimpath -tags timetzdata
LDFLAGS="-X main.buildTs=`date -u +%y%m%d_%H%m%S_%Z`"

export GOOS
export GOARCH
export CGO_ENABLED

all: echoMode genMock buildBin

buildBin:
	go build $(BUILD_OPTS) -ldflags $(LDFLAGS) -o gmqcli.bin gmq/cmd/gmqcli/main.go

genMock:
	go install github.com/golang/mock/mockgen@v1.6.0
	mockgen -source=gmq/broker.go -destination=gmq/broker_mock.go -package=gmq -mock_names Interface=ImplMock

echoMode:
	@echo GOOS=$(GOOS)
	@echo GOARCH=$(GOARCH)
	@echo CGO_ENABLED=$(CGO_ENABLED)
	@echo MODE=$(MODE)
	@echo LDFLAGS=$(LDFLAGS)

# Override with make test TEST_TIMEOUT=2m.
TEST_TIMEOUT?=5m
.PHONY: test
test:
	go test -timeout=$(TEST_TIMEOUT) ./...
