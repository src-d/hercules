all: ${GOPATH}/src/gopkg.in/bblfsh/client-go.v2 ${GOPATH}/bin/hercules

test: all
	go test gopkg.in/src-d/hercules.v3

.ONESHELL:
${GOPATH}/src/gopkg.in/bblfsh/client-go.v2:
	go get -v gopkg.in/bblfsh/client-go.v2/...
	cd $$GOPATH/src/gopkg.in/bblfsh/client-go.v2
	make dependencies

.ONESHELL:
${GOPATH}/bin/hercules:
	cd ${GOPATH}/src/gopkg.in/src-d/hercules.v3
	go get -ldflags "-X gopkg.in/src-d/hercules.v3.GIT_HASH=$$(git rev-parse HEAD)" gopkg.in/src-d/hercules.v3/cmd/hercules
	${GOPATH}/bin/hercules -version
