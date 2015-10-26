package main

import (
	logic "SpartaApplication/logic"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/lambda"
	sparta "github.com/mweagle/Sparta"
	"net/http"
)

const LAMBDA_EXECUTE_ROLE_NAME = "LambdaExecutor"

func helloWorld(event sparta.LambdaEvent, context sparta.LambdaContext, w http.ResponseWriter) {
	fmt.Fprintf(w, "Hello World! Data: %s", event)
}

func sampleData() []*sparta.LambdaAWSInfo {
	var lambdaFunctions []*sparta.LambdaAWSInfo
	lambdaFn := sparta.NewLambda(LAMBDA_EXECUTE_ROLE_NAME, mockLambda1, nil)
	lambdaFn.Permissions = append(lambdaFn.Permissions, &lambda.AddPermissionInput{
		Action:      aws.String("lambda:InvokeFunction"),
		Principal:   aws.String("s3.amazonaws.com"),
		StatementId: aws.String("MyUniqueID"),
		SourceArn:   aws.String("arn:aws:s3:::someBucket"),
	})
	lambdaFn.EventSourceMappings = append(lambdaFn.EventSourceMappings, &lambda.CreateEventSourceMappingInput{
		EventSourceArn:   aws.String("arn:aws:dynamodb:us-west-2:123412341234:table/coconut-browswer/stream/2015-10-22T15:05:13.779"),
		StartingPosition: aws.String("TRIM_HORIZON"),
		BatchSize:        aws.Int64(10),
		Enabled:          aws.Bool(true),
	})
	lambdaFunctions = append(lambdaFunctions, lambdaFn)
	lambdaFunctions = append(lambdaFunctions, sparta.NewLambda(LAMBDA_EXECUTE_ROLE_NAME, logic.LogicLambda1, nil))
	return lambdaFunctions
}

func main() {
	sparta.Main("Sparta Application", "This is a sample Sparta application", sampleData())
}
