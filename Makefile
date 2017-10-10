.DEFAULT_GOAL=build
.PHONY: build test run

vet:
	go vet .

generate:
	go generate -x

get:
	go get -u github.com/golang/dep/...
	dep ensure

build: get vet generate
	go build .

clean:
	go clean .

test: build
	go test .

delete:
	go run main.go delete

explore:
	go run main.go --level info explore

provision:
	go run main.go provision --s3Bucket $(S3_BUCKET)

describe: build
	KINESIS_TEST_STREAM="" S3_TEST_BUCKET="" SNS_TEST_TOPIC="" DYNAMO_TEST_STREAM="" go run main.go --level info describe --s3Bucket $(S3_BUCKET) --out ./graph.html
