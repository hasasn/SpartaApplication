package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/lambda"
	sparta "github.com/mweagle/Sparta"
	spartaDynamoDB "github.com/mweagle/Sparta/aws/dynamodb"
	spartaKinesis "github.com/mweagle/Sparta/aws/kinesis"
	spartaSNS "github.com/mweagle/Sparta/aws/sns"
)

////////////////////////////////////////////////////////////////////////////////
func paramVal(keyName string, defaultValue string) string {
	value := os.Getenv(keyName)
	if "" == value {
		value = defaultValue
	}
	return value
}

var s3Bucket = paramVal("S3_TEST_BUCKET", "arn:aws:s3:::MyS3Bucket")
var snsTopic = paramVal("SNS_TEST_TOPIC", "arn:aws:sns:us-west-2:123412341234:mySNSTopic")
var dynamoTestStream = paramVal("DYNAMO_TEST_STREAM", "arn:aws:dynamodb:us-west-2:123412341234:table/myTableName/stream/2015-10-22T15:05:13.779")
var kinesisTestStream = paramVal("KINESIS_TEST_STREAM", "arn:aws:kinesis:us-west-2:123412341234:stream/kinesisTestStream")

////////////////////////////////////////////////////////////////////////////////
// S3 handler
//

func echoS3Event(event *json.RawMessage, context *sparta.LambdaContext, w http.ResponseWriter, logger *logrus.Logger) {
	logger.WithFields(logrus.Fields{
		"RequestID": context.AWSRequestID,
		"Event":     string(*event),
	}).Info("Request received")

	fmt.Fprintf(w, string(*event))
}

func appendS3Lambda(api *sparta.API, lambdaFunctions []*sparta.LambdaAWSInfo) []*sparta.LambdaAWSInfo {
	lambdaFn := sparta.NewLambda(sparta.IAMRoleDefinition{}, echoS3Event, nil)
	apiGatewayResource, _ := api.NewResource("/hello/world/test", lambdaFn)
	apiGatewayResource.NewMethod("GET")

	lambdaFn.Permissions = append(lambdaFn.Permissions, sparta.S3Permission{
		BasePermission: sparta.BasePermission{
			SourceArn: s3Bucket,
		},
		Events: []string{"s3:ObjectCreated:*", "s3:ObjectRemoved:*"},
	})
	return append(lambdaFunctions, lambdaFn)
}

////////////////////////////////////////////////////////////////////////////////
// SNS handler
//
func echoSNSEvent(event *json.RawMessage, context *sparta.LambdaContext, w http.ResponseWriter, logger *logrus.Logger) {
	logger.WithFields(logrus.Fields{
		"RequestID": context.AWSRequestID,
	}).Info("Request received")

	var lambdaEvent spartaSNS.Event
	err := json.Unmarshal([]byte(*event), &lambdaEvent)
	if err != nil {
		logger.Error("Failed to unmarshal event data: ", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	for _, eachRecord := range lambdaEvent.Records {
		logger.WithFields(logrus.Fields{
			"Subject": eachRecord.Sns.Subject,
			"Message": eachRecord.Sns.Message,
		}).Info("SNS Event")
	}
}

func appendSNSLambda(api *sparta.API, lambdaFunctions []*sparta.LambdaAWSInfo) []*sparta.LambdaAWSInfo {
	lambdaFn := sparta.NewLambda(sparta.IAMRoleDefinition{}, echoSNSEvent, nil)
	lambdaFn.Permissions = append(lambdaFn.Permissions, sparta.SNSPermission{
		BasePermission: sparta.BasePermission{
			SourceArn: snsTopic,
		},
	})
	return append(lambdaFunctions, lambdaFn)
}

////////////////////////////////////////////////////////////////////////////////
// DynamoDB handler
//
func echoDynamoDBEvent(event *json.RawMessage, context *sparta.LambdaContext, w http.ResponseWriter, logger *logrus.Logger) {
	logger.WithFields(logrus.Fields{
		"RequestID": context.AWSRequestID,
		"Event":     string(*event),
	}).Info("Request received")

	var lambdaEvent spartaDynamoDB.Event
	err := json.Unmarshal([]byte(*event), &lambdaEvent)
	if err != nil {
		logger.Error("Failed to unmarshal event data: ", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	for _, eachRecord := range lambdaEvent.Records {
		logger.WithFields(logrus.Fields{
			"Keys":     eachRecord.DynamoDB.Keys,
			"NewImage": eachRecord.DynamoDB.NewImage,
		}).Info("DynamoDb Event")
	}

	fmt.Fprintf(w, "Done!")
}

func appendDynamoDBLambda(api *sparta.API, lambdaFunctions []*sparta.LambdaAWSInfo) []*sparta.LambdaAWSInfo {
	lambdaFn := sparta.NewLambda(sparta.IAMRoleDefinition{}, echoDynamoDBEvent, nil)
	lambdaFn.EventSourceMappings = append(lambdaFn.EventSourceMappings, &lambda.CreateEventSourceMappingInput{
		EventSourceArn:   aws.String(dynamoTestStream),
		StartingPosition: aws.String("TRIM_HORIZON"),
		BatchSize:        aws.Int64(10),
		Enabled:          aws.Bool(true),
	})
	return append(lambdaFunctions, lambdaFn)
}

////////////////////////////////////////////////////////////////////////////////
// Kinesis handler
//
func echoKinesisEvent(event *json.RawMessage, context *sparta.LambdaContext, w http.ResponseWriter, logger *logrus.Logger) {
	logger.WithFields(logrus.Fields{
		"RequestID": context.AWSRequestID,
		"Event":     string(*event),
	}).Info("Request received")

	var lambdaEvent spartaKinesis.Event
	err := json.Unmarshal([]byte(*event), &lambdaEvent)
	if err != nil {
		logger.Error("Failed to unmarshal event data: ", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	for _, eachRecord := range lambdaEvent.Records {
		logger.WithFields(logrus.Fields{
			"EventID": eachRecord.EventID,
		}).Info("Kinesis Event")
	}
}

func appendKinesisLambda(api *sparta.API, lambdaFunctions []*sparta.LambdaAWSInfo) []*sparta.LambdaAWSInfo {
	lambdaFn := sparta.NewLambda(sparta.IAMRoleDefinition{}, echoKinesisEvent, nil)
	lambdaFn.EventSourceMappings = append(lambdaFn.EventSourceMappings, &lambda.CreateEventSourceMappingInput{
		EventSourceArn:   aws.String(kinesisTestStream),
		StartingPosition: aws.String("TRIM_HORIZON"),
		BatchSize:        aws.Int64(100),
		Enabled:          aws.Bool(true),
	})
	return append(lambdaFunctions, lambdaFn)
}

////////////////////////////////////////////////////////////////////////////////
// Return the *[]sparta.LambdaAWSInfo slice
//
func spartaLambdaData(api *sparta.API) []*sparta.LambdaAWSInfo {

	var lambdaFunctions []*sparta.LambdaAWSInfo
	lambdaFunctions = appendS3Lambda(api, lambdaFunctions)
	lambdaFunctions = appendSNSLambda(api, lambdaFunctions)
	lambdaFunctions = appendDynamoDBLambda(api, lambdaFunctions)
	lambdaFunctions = appendKinesisLambda(api, lambdaFunctions)
	return lambdaFunctions
}

func main() {
	stage := sparta.NewStage("prod")
	apiGateway := sparta.NewAPIGateway("MySpartaAPI", stage)
	stackName := "SpartaApplication"
	sparta.Main(stackName,
		"Simple Sparta application",
		spartaLambdaData(apiGateway),
		apiGateway)
}
