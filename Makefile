GOPATH ?= $(shell go env GOPATH)
ifneq ($(OS),Windows_NT)
EXE =
else
EXE = .exe
endif
PKG = $(shell go env GOOS)_$(shell go env GOARCH)
ifneq (${DISABLE_TENSORFLOW},1)
TAGS ?= tensorflow
endif

all: ${GOPATH}/bin/hercules${EXE}

test: all
	go test gopkg.in/src-d/hercules.v3

${GOPATH}/bin/protoc-gen-gogo${EXE}:
	go get -v github.com/gogo/protobuf/protoc-gen-gogo

ifneq ($(OS),Windows_NT)
pb/pb.pb.go: pb/pb.proto ${GOPATH}/bin/protoc-gen-gogo
	PATH=${PATH}:${GOPATH}/bin protoc --gogo_out=pb --proto_path=pb pb/pb.proto
else
pb/pb.pb.go: pb/pb.proto ${GOPATH}/bin/protoc-gen-gogo.exe
	set "PATH=${PATH};${GOPATH}\bin" && \
	call protoc --gogo_out=pb --proto_path=pb pb/pb.proto
endif

pb/pb_pb2.py: pb/pb.proto
	protoc --python_out pb --proto_path=pb pb/pb.proto

cmd/hercules/plugin_template_source.go: cmd/hercules/plugin.template
	cd cmd/hercules && go generate

${GOPATH}/src/gopkg.in/bblfsh/client-go.v2:
	go get -d -v gopkg.in/bblfsh/client-go.v2/...

${GOPATH}/pkg/$(PKG)/gopkg.in/bblfsh/client-go.v2: ${GOPATH}/src/gopkg.in/bblfsh/client-go.v2
	cd ${GOPATH}/src/gopkg.in/bblfsh/client-go.v2 && \
	make dependencies

${GOPATH}/bin/hercules${EXE}: *.go cmd/hercules/*.go rbtree/*.go yaml/*.go toposort/*.go pb/*.go ${GOPATH}/pkg/$(PKG)/gopkg.in/bblfsh/client-go.v2 pb/pb.pb.go pb/pb_pb2.py cmd/hercules/plugin_template_source.go
	go get -tags "$(TAGS)" -ldflags "-X gopkg.in/src-d/hercules.v3.BinaryGitHash=$(shell git rev-parse HEAD)" gopkg.in/src-d/hercules.v3/cmd/hercules
