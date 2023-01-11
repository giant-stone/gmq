set GOOS=windows
set GOARCH=amd64
set BUILD_OPTS=-trimpath -tags timetzdata

go build %BUILD_OPTS% -o gmqcli.exe gmq\cmd\gmqcli\main.go
