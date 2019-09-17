GOBIN ?= .
GO111MODULE=on
ifneq ($(OS),Windows_NT)
EXE =
else
EXE = .exe
endif
PKG = $(shell go env GOOS)_$(shell go env GOARCH)
TAGS ?=

all: ${GOBIN}/hercules${EXE}

test: all
	go test gopkg.in/src-d/hercules.v10

${GOBIN}/protoc-gen-gogo${EXE}:
	go build github.com/gogo/protobuf/protoc-gen-gogo

ifneq ($(OS),Windows_NT)
internal/pb/pb.pb.go: internal/pb/pb.proto ${GOBIN}/protoc-gen-gogo
	PATH=${PATH}:${GOBIN} protoc --gogo_out=internal/pb --proto_path=internal/pb internal/pb/pb.proto
else
internal/pb/pb.pb.go: internal/pb/pb.proto ${GOBIN}/protoc-gen-gogo.exe
	export PATH="${PATH};${GOBIN}" && \
	protoc --gogo_out=internal/pb --proto_path=internal/pb internal/pb/pb.proto
endif

python/labours/pb_pb2.py: internal/pb/pb.proto
	protoc --python_out python/labours --proto_path=internal/pb internal/pb/pb.proto

cmd/hercules/plugin_template_source.go: cmd/hercules/plugin.template
	cd cmd/hercules && go generate

vendor:
	go mod vendor

${GOBIN}/hercules${EXE}: vendor *.go */*.go */*/*.go */*/*/*.go internal/pb/pb.pb.go python/labours/pb_pb2.py cmd/hercules/plugin_template_source.go
	go build -tags "$(TAGS)" -ldflags "-X gopkg.in/src-d/hercules.v10.BinaryGitHash=$(shell git rev-parse HEAD)" gopkg.in/src-d/hercules.v10/cmd/hercules
