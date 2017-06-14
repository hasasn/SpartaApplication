package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/Sirupsen/logrus"
	sparta "github.com/mweagle/Sparta"
	spartaCF "github.com/mweagle/Sparta/aws/cloudformation"
	spartaDynamoDB "github.com/mweagle/Sparta/aws/dynamodb"
	spartaKinesis "github.com/mweagle/Sparta/aws/kinesis"
	spartaSES "github.com/mweagle/Sparta/aws/ses"
	spartaSNS "github.com/mweagle/Sparta/aws/sns"
	gocf "github.com/mweagle/go-cloudformation"
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
	apiGatewayResource.NewMethod("GET", http.StatusOK)

	lambdaFn.Permissions = append(lambdaFn.Permissions, sparta.S3Permission{
		BasePermission: sparta.BasePermission{
			SourceArn: s3Bucket,
		},
		Events: []string{"s3:ObjectCreated:*", "s3:ObjectRemoved:*"},
	})
	return append(lambdaFunctions, lambdaFn)
}

func echoS3DynamicBucketEvent(event *json.RawMessage,
	context *sparta.LambdaContext,
	w http.ResponseWriter,
	logger *logrus.Logger) {

	config, _ := sparta.Discover()
	logger.WithFields(logrus.Fields{
		"RequestID":     context.AWSRequestID,
		"Event":         string(*event),
		"Configuration": config,
	}).Info("Request received")

	fmt.Fprintf(w, string(*event))
}

func appendDynamicS3BucketLambda(api *sparta.API, lambdaFunctions []*sparta.LambdaAWSInfo) []*sparta.LambdaAWSInfo {

	s3BucketResourceName := sparta.CloudFormationResourceName("S3DynamicBucket")

	lambdaFn := sparta.NewLambda(sparta.IAMRoleDefinition{}, echoS3DynamicBucketEvent, nil)
	lambdaFn.Permissions = append(lambdaFn.Permissions, sparta.S3Permission{
		BasePermission: sparta.BasePermission{
			SourceArn: gocf.Ref(s3BucketResourceName),
		},
		Events: []string{"s3:ObjectCreated:*", "s3:ObjectRemoved:*"},
	})
	lambdaFn.DependsOn = append(lambdaFn.DependsOn, s3BucketResourceName)

	// Add permission s.t. the lambda function could read from the S3 bucket
	lambdaFn.RoleDefinition.Privileges = append(lambdaFn.RoleDefinition.Privileges,
		sparta.IAMRolePrivilege{
			Actions:  []string{"s3:GetObject", "s3:HeadObject"},
			Resource: spartaCF.S3AllKeysArnForBucket(gocf.Ref(s3BucketResourceName)),
		})

	lambdaFn.Decorator = func(serviceName string,
		lambdaResourceName string,
		lambdaResource gocf.LambdaFunction,
		resourceMetadata map[string]interface{},
		S3Bucket string,
		S3Key string,
		buildID string,
		template *gocf.Template,
		context map[string]interface{},
		logger *logrus.Logger) error {
		cfResource := template.AddResource(s3BucketResourceName, &gocf.S3Bucket{
			AccessControl: gocf.String("PublicRead"),
			Tags: &gocf.TagList{gocf.Tag{
				Key:   gocf.String("SpecialKey"),
				Value: gocf.String("SpecialValue"),
			},
			},
		})
		cfResource.DeletionPolicy = "Delete"
		return nil
	}
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

func echoDynamicSNSEvent(event *json.RawMessage, context *sparta.LambdaContext, w http.ResponseWriter, logger *logrus.Logger) {
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

func appendDynamicSNSLambda(api *sparta.API, lambdaFunctions []*sparta.LambdaAWSInfo) []*sparta.LambdaAWSInfo {
	snsTopicName := sparta.CloudFormationResourceName("SNSDynamicTopic")
	lambdaFn := sparta.NewLambda(sparta.IAMRoleDefinition{}, echoDynamicSNSEvent, nil)
	lambdaFn.Permissions = append(lambdaFn.Permissions, sparta.SNSPermission{
		BasePermission: sparta.BasePermission{
			SourceArn: gocf.Ref(snsTopicName),
		},
	})

	lambdaFn.Decorator = func(serviceName string,
		lambdaResourceName string,
		lambdaResource gocf.LambdaFunction,
		resourceMetadata map[string]interface{},
		S3Bucket string,
		S3Key string,
		buildID string,
		template *gocf.Template,
		context map[string]interface{},
		logger *logrus.Logger) error {
		template.AddResource(snsTopicName, &gocf.SNSTopic{
			DisplayName: gocf.String("Sparta Application SNS topic"),
		})
		return nil
	}
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
	lambdaFn.EventSourceMappings = append(lambdaFn.EventSourceMappings, &sparta.EventSourceMapping{
		EventSourceArn:   dynamoTestStream,
		StartingPosition: "TRIM_HORIZON",
		BatchSize:        10,
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
	lambdaFn.EventSourceMappings = append(lambdaFn.EventSourceMappings, &sparta.EventSourceMapping{
		EventSourceArn:   kinesisTestStream,
		StartingPosition: "TRIM_HORIZON",
		BatchSize:        100,
	})
	return append(lambdaFunctions, lambdaFn)
}

////////////////////////////////////////////////////////////////////////////////
// SES handler
//
func echoSESEvent(event *json.RawMessage,
	context *sparta.LambdaContext,
	w http.ResponseWriter,
	logger *logrus.Logger) {

	logger.WithFields(logrus.Fields{
		"RequestID": context.AWSRequestID,
	}).Info("Request received")

	configuration, configErr := sparta.Discover()
	logger.WithFields(logrus.Fields{
		"Error":         configErr,
		"Configuration": configuration,
	}).Info("Discovery results")

	// The message bucket is an explicit `DependsOn` relationship, so it'll be in the
	// resources map.  We'll find it by looking for the dependent resource with the "AWS::S3::Bucket" type
	bucketName := ""
	for _, eachResource := range configuration.Resources {
		if eachResource.Properties[sparta.TagResourceType] == "AWS::S3::Bucket" {
			bucketName = eachResource.Properties["Ref"]
		}
	}
	if "" == bucketName {
		logger.Error("Failed to discover SES bucket from sparta.Discovery")
		http.Error(w, "Failed to discovery SES MessageBodyBucket", http.StatusInternalServerError)
	}
	// The bucket is in the configuration map, prefixed by
	// SESMessageStoreBucket

	var lambdaEvent spartaSES.Event
	err := json.Unmarshal([]byte(*event), &lambdaEvent)
	if err != nil {
		logger.Error("Failed to unmarshal event data: ", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	// Get the metdata about the item...
	svc := s3.New(session.New())
	for _, eachRecord := range lambdaEvent.Records {
		logger.WithFields(logrus.Fields{
			"Source":     eachRecord.SES.Mail.Source,
			"MessageID":  eachRecord.SES.Mail.MessageID,
			"BucketName": bucketName,
		}).Info("SES Event")

		if "" != bucketName {
			params := &s3.HeadObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(eachRecord.SES.Mail.MessageID),
			}
			resp, err := svc.HeadObject(params)
			logger.WithFields(logrus.Fields{
				"Error":    err,
				"Metadata": resp,
			}).Info("SES MessageBody")
		}
	}
}

func appendSESLambda(api *sparta.API,
	lambdaFunctions []*sparta.LambdaAWSInfo) []*sparta.LambdaAWSInfo {

	// Setup options s.t. the lambda function has time to consume the message body
	sesItemInfoOptions := &sparta.LambdaFunctionOptions{
		Description: "",
		MemorySize:  128,
		Timeout:     10,
	}
	// Our lambda function will need to be able to read from the bucket, which
	// will be handled by the S3MessageBodyBucketDecorator below
	lambdaFn := sparta.NewLambda(sparta.IAMRoleDefinition{}, echoSESEvent, sesItemInfoOptions)

	// Add a Permission s.t. the Lambda function automatically manages SES registration
	sesPermission := sparta.SESPermission{
		BasePermission: sparta.BasePermission{
			SourceArn: "*",
		},
		InvocationType: "Event",
	}
	// Store the message body
	bodyStorage, _ := sesPermission.NewMessageBodyStorageResource("Special")
	sesPermission.MessageBodyStorage = bodyStorage

	sesPermission.ReceiptRules = make([]sparta.ReceiptRule, 0)
	sesPermission.ReceiptRules = append(sesPermission.ReceiptRules, sparta.ReceiptRule{
		Name:       "Special",
		Recipients: []string{"sombody_special@gosparta.io"},
		TLSPolicy:  "Optional",
	})
	sesPermission.ReceiptRules = append(sesPermission.ReceiptRules, sparta.ReceiptRule{
		Name:       "Default",
		Recipients: []string{},
		TLSPolicy:  "Optional",
	})

	// Then add the privilege to the Lambda function s.t. we can actually get at the data
	lambdaFn.RoleDefinition.Privileges = append(lambdaFn.RoleDefinition.Privileges,
		sparta.IAMRolePrivilege{
			Actions:  []string{"s3:GetObject", "s3:HeadObject"},
			Resource: sesPermission.MessageBodyStorage.BucketArnAllKeys(),
		})

	// Finally add the SES permission to the lambda function
	lambdaFn.Permissions = append(lambdaFn.Permissions, sesPermission)

	return append(lambdaFunctions, lambdaFn)
}

////////////////////////////////////////////////////////////////////////////////
// CloudWatchEvent handler
//
func echoCloudWatchEvent(event *json.RawMessage, context *sparta.LambdaContext, w http.ResponseWriter, logger *logrus.Logger) {
	logger.WithFields(logrus.Fields{
		"RequestID": context.AWSRequestID,
	}).Info("Request received")

	config, _ := sparta.Discover()
	logger.WithFields(logrus.Fields{
		"RequestID":     context.AWSRequestID,
		"Event":         string(*event),
		"Configuration": config,
	}).Info("Request received")
	fmt.Fprintf(w, "Hello World!")
}
func appendCloudWatchEventHandler(api *sparta.API,
	lambdaFunctions []*sparta.LambdaAWSInfo) []*sparta.LambdaAWSInfo {

	lambdaFn := sparta.NewLambda(sparta.IAMRoleDefinition{},
		echoCloudWatchEvent,
		nil)
	cloudWatchEventsPermission := sparta.CloudWatchEventsPermission{}
	cloudWatchEventsPermission.Rules = make(map[string]sparta.CloudWatchEventsRule, 0)
	cloudWatchEventsPermission.Rules["Rate5Mins"] = sparta.CloudWatchEventsRule{
		ScheduleExpression: "rate(5 minutes)",
	}
	cloudWatchEventsPermission.Rules["EC2Activity"] = sparta.CloudWatchEventsRule{
		EventPattern: map[string]interface{}{
			"source":      []string{"aws.ec2"},
			"detail-type": []string{"EC2 Instance state change"},
		},
	}
	lambdaFn.Permissions = append(lambdaFn.Permissions, cloudWatchEventsPermission)

	return append(lambdaFunctions, lambdaFn)
}

////////////////////////////////////////////////////////////////////////////////
// CloudWatchEvent handler
//
func echoCloudWatchLogs(event *json.RawMessage, context *sparta.LambdaContext, w http.ResponseWriter, logger *logrus.Logger) {
	logger.WithFields(logrus.Fields{
		"RequestID": context.AWSRequestID,
	}).Info("Request received")
	fmt.Fprintf(w, "Hello World!")
}

func appendCloudWatchLogsHandler(api *sparta.API,
	lambdaFunctions []*sparta.LambdaAWSInfo) []*sparta.LambdaAWSInfo {

	lambdaFn := sparta.NewLambda(sparta.IAMRoleDefinition{},
		echoCloudWatchLogs,
		nil)
	cloudWatchLogsPermission := sparta.CloudWatchLogsPermission{}
	cloudWatchLogsPermission.Filters = make(map[string]sparta.CloudWatchLogsSubscriptionFilter, 1)
	cloudWatchLogsPermission.Filters["MyFilter"] = sparta.CloudWatchLogsSubscriptionFilter{
		FilterPattern: "",
		LogGroupName:  "/aws/lambda/versions",
	}
	lambdaFn.Permissions = append(lambdaFn.Permissions, cloudWatchLogsPermission)
	return append(lambdaFunctions, lambdaFn)
}

////////////////////////////////////////////////////////////////////////////////
// Return the *[]sparta.LambdaAWSInfo slice
//
func spartaLambdaData(api *sparta.API) []*sparta.LambdaAWSInfo {

	var lambdaFunctions []*sparta.LambdaAWSInfo
	lambdaFunctions = appendS3Lambda(api, lambdaFunctions)
	lambdaFunctions = appendDynamicS3BucketLambda(api, lambdaFunctions)
	lambdaFunctions = appendSNSLambda(api, lambdaFunctions)
	lambdaFunctions = appendDynamicSNSLambda(api, lambdaFunctions)
	lambdaFunctions = appendDynamoDBLambda(api, lambdaFunctions)
	lambdaFunctions = appendKinesisLambda(api, lambdaFunctions)
	lambdaFunctions = appendSESLambda(api, lambdaFunctions)
	lambdaFunctions = appendCloudWatchEventHandler(api, lambdaFunctions)
	lambdaFunctions = appendCloudWatchLogsHandler(api, lambdaFunctions)
	return lambdaFunctions
}

func main() {
	stage := sparta.NewStage("prod")
	apiGateway := sparta.NewAPIGateway("MySpartaAPI", stage)
	apiGateway.CORSEnabled = true

	stackName := "SpartaApplication"
	sparta.Main(stackName,
		"Simple Sparta application",
		spartaLambdaData(apiGateway),
		apiGateway,
		nil)
}
