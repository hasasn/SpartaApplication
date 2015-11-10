package main

import (
	"encoding/json"
	"net/http"
	"os"

	"github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/lambda"
	sparta "github.com/mweagle/Sparta"
)

////////////////////////////////////////////////////////////////////////////////
func paramVal(keyName string, defaultValue string) string {
	value := os.Getenv(keyName)
	if "" == value {
		value = defaultValue
	}
	return value
}

var S3_BUCKET = paramVal("S3_TEST_BUCKET", "arn:aws:s3:::MyS3Bucket")
var SNS_TOPIC = paramVal("SNS_TEST_TOPIC", "arn:aws:sns:us-west-2:123412341234:mySNSTopic")
var DYNAMO_EVENT_SOURCE = paramVal("DYNAMO_TEST_STREAM", "arn:aws:dynamodb:us-west-2:123412341234:table/myTableName/stream/2015-10-22T15:05:13.779")

////////////////////////////////////////////////////////////////////////////////
// Echo handler
//
func echoEvent(event *json.RawMessage, context *sparta.LambdaContext, w *http.ResponseWriter, logger *logrus.Logger) {
	logger.WithFields(logrus.Fields{
		"RequestID": context.AWSRequestID,
		"Event":     string(*event),
	}).Info("Request received")
}

////////////////////////////////////////////////////////////////////////////////
// Return the *[]sparta.LambdaAWSInfo slice
//
func spartaLambdaData() []*sparta.LambdaAWSInfo {

	// Provision an IAM::Role as part of this application
	var iamRole = sparta.IAMRoleDefinition{}
	iamRole.Privileges = append(iamRole.Privileges, sparta.IAMRolePrivilege{
		Actions: []string{"s3:GetObject",
			"s3:PutObject",
		},
		Resource: S3_BUCKET,
	})

	var lambdaFunctions []*sparta.LambdaAWSInfo
	lambdaFn := sparta.NewLambda(iamRole, echoEvent, nil)

	//////////////////////////////////////////////////////////////////////////////
	// S3 configuration
	//
	lambdaFn.Permissions = append(lambdaFn.Permissions, sparta.S3Permission{
		BasePermission: sparta.BasePermission{
			SourceArn: S3_BUCKET,
		},
		Events: []string{"s3:ObjectCreated:*", "s3:ObjectRemoved:*"},
	})

	//////////////////////////////////////////////////////////////////////////////
	// SNS configuration
	//
	lambdaFn.Permissions = append(lambdaFn.Permissions, sparta.SNSPermission{
		BasePermission: sparta.BasePermission{
			SourceArn: SNS_TOPIC,
		},
	})

	//////////////////////////////////////////////////////////////////////////////
	// Dynamo configuration
	//
	lambdaFn.EventSourceMappings = append(lambdaFn.EventSourceMappings, &lambda.CreateEventSourceMappingInput{
		EventSourceArn:   aws.String(DYNAMO_EVENT_SOURCE),
		StartingPosition: aws.String("TRIM_HORIZON"),
		BatchSize:        aws.Int64(10),
		Enabled:          aws.Bool(true),
	})
	lambdaFunctions = append(lambdaFunctions, lambdaFn)
	return lambdaFunctions
}

func main() {
	stackName := "SpartaApplication"
	sparta.Main(stackName, "This is a sample Sparta application", spartaLambdaData())
}
