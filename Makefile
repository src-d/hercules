ifneq (oneshell, $(findstring oneshell, $(.FEATURES)))
  $(error GNU make 3.82 or later is required)
endif

all: ${GOPATH}/bin/hercules

test: all
	go test gopkg.in/src-d/hercules.v3

dependencies: ${GOPATH}/src/gopkg.in/bblfsh/client-go.v2 ${GOPATH}/src/gopkg.in/src-d/hercules.v3 ${GOPATH}/src/gopkg.in/src-d/hercules.v3/pb/pb.pb.go ${GOPATH}/src/gopkg.in/src-d/hercules.v3/pb/pb_pb2.py ${GOPATH}/src/gopkg.in/src-d/hercules.v3/cmd/hercules/plugin_template_source.go

${GOPATH}/src/gopkg.in/src-d/hercules.v3/pb/pb.pb.go: pb/pb.proto
	PATH=$$PATH:$$GOPATH/bin protoc --gogo_out=pb --proto_path=pb pb/pb.proto

${GOPATH}/src/gopkg.in/src-d/hercules.v3/pb/pb_pb2.py: pb/pb.proto
	protoc --python_out pb --proto_path=pb pb/pb.proto

${GOPATH}/src/gopkg.in/src-d/hercules.v3/cmd/hercules/plugin_template_source.go: ${GOPATH}/src/gopkg.in/src-d/hercules.v3/cmd/hercules/plugin.template
	cd ${GOPATH}/src/gopkg.in/src-d/hercules.v3/cmd/hercules && go generate

${GOPATH}/src/gopkg.in/src-d/hercules.v3:
	go get -d gopkg.in/src-d/hercules.v3/...

.ONESHELL:
${GOPATH}/src/gopkg.in/bblfsh/client-go.v2:
	go get -v gopkg.in/bblfsh/client-go.v2/... || true
	cd $$GOPATH/src/gopkg.in/bblfsh/client-go.v2
	make dependencies

.ONESHELL:
${GOPATH}/bin/hercules: dependencies *.go cmd/hercules/*.go rbtree/*.go yaml/*.go toposort/*.go pb/*.go
	cd ${GOPATH}/src/gopkg.in/src-d/hercules.v3
	go get -ldflags "-X gopkg.in/src-d/hercules.v3.BinaryGitHash=$$(git rev-parse HEAD)" gopkg.in/src-d/hercules.v3/cmd/hercules
