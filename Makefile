GOPATH ?= $(shell go env GOPATH)
ifneq ($(OS),Windows_NT)
EXE =
else
EXE = .exe
endif
PKG = $(shell go env GOOS)_$(shell go env GOARCH)
TAGS ?=

all: ${GOPATH}/bin/hercules${EXE}

test: all
	go test gopkg.in/src-d/hercules.v8

${GOPATH}/bin/protoc-gen-gogo${EXE}:
	go get -v github.com/gogo/protobuf/protoc-gen-gogo

ifneq ($(OS),Windows_NT)
internal/pb/pb.pb.go: internal/pb/pb.proto ${GOPATH}/bin/protoc-gen-gogo
	PATH=${PATH}:${GOPATH}/bin protoc --gogo_out=internal/pb --proto_path=internal/pb internal/pb/pb.proto
else
internal/pb/pb.pb.go: internal/pb/pb.proto ${GOPATH}/bin/protoc-gen-gogo.exe
	export PATH="${PATH};${GOPATH}\bin" && \
	protoc --gogo_out=internal/pb --proto_path=internal/pb internal/pb/pb.proto
endif

internal/pb/pb_pb2.py: internal/pb/pb.proto
	protoc --python_out internal/pb --proto_path=internal/pb internal/pb/pb.proto

cmd/hercules/plugin_template_source.go: cmd/hercules/plugin.template
	cd cmd/hercules && go generate

vendor:
	dep ensure -v

${GOPATH}/bin/hercules${EXE}: vendor *.go */*.go */*/*.go */*/*/*.go internal/pb/pb.pb.go internal/pb/pb_pb2.py cmd/hercules/plugin_template_source.go
	go get -tags "$(TAGS)" -ldflags "-X gopkg.in/src-d/hercules.v8.BinaryGitHash=$(shell git rev-parse HEAD)" gopkg.in/src-d/hercules.v8/cmd/hercules
