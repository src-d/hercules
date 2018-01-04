all: churn_analysis.so

churn_analysis.so: churn_analysis.go churn_analysis.pb.go
	go build -buildmode=plugin -linkshared churn_analysis.go churn_analysis.pb.go

churn_analysis.pb.go: churn_analysis.proto
	PATH=$$PATH:$$GOPATH/bin protoc --gogo_out=. --proto_path=. churn_analysis.proto
