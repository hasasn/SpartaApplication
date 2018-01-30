package main

import (
	"context"
	"net/http"
	"os"

	awsLambdaEvents "github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	sparta "github.com/mweagle/Sparta"
	spartaCF "github.com/mweagle/Sparta/aws/cloudformation"
	spartaSES "github.com/mweagle/Sparta/aws/ses"
	gocf "github.com/mweagle/go-cloudformation"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

////////////////////////////////////////////////////////////////////////////////
func paramVal(keyName string, defaultValue string) string {
	value := os.Getenv(keyName)
	if "" == value {
		value = defaultValue
	}
	return value
}

var s3Bucket = paramVal("SPARTA_S3_TEST_BUCKET", "arn:aws:s3:::MyS3Bucket")
var snsTopic = paramVal("SPARTA_SNS_TEST_TOPIC", "arn:aws:sns:us-west-2:123412341234:mySNSTopic")
var dynamoTestStream = paramVal("SPARTA_DYNAMO_TEST_STREAM", "arn:aws:dynamodb:us-west-2:123412341234:table/myTableName/stream/2015-10-22T15:05:13.779")
var kinesisTestStream = paramVal("SPARTA_KINESIS_TEST_STREAM", "arn:aws:kinesis:us-west-2:123412341234:stream/kinesisTestStream")

////////////////////////////////////////////////////////////////////////////////
// S3 handler
//
func echoS3Event(ctx context.Context, s3Event awsLambdaEvents.S3Event) (*awsLambdaEvents.S3Event, error) {
	logger, _ := ctx.Value(sparta.ContextKeyRequestLogger).(*logrus.Entry)
	logger.WithFields(logrus.Fields{
		"Event": s3Event,
	}).Info("Event received")
	return &s3Event, nil
}

func appendS3Lambda(api *sparta.API, lambdaFunctions []*sparta.LambdaAWSInfo) []*sparta.LambdaAWSInfo {
	lambdaFn := sparta.HandleAWSLambda(sparta.LambdaName(echoS3Event),
		echoS3Event,
		sparta.IAMRoleDefinition{})
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

func echoS3DynamicBucketEvent(ctx context.Context,
	s3Event awsLambdaEvents.S3Event) (*awsLambdaEvents.S3Event, error) {
	logger, _ := ctx.Value(sparta.ContextKeyRequestLogger).(*logrus.Entry)
	discoveryInfo, discoveryInfoErr := sparta.Discover()
	logger.WithFields(logrus.Fields{
		"Event":        s3Event,
		"Discovery":    discoveryInfo,
		"DiscoveryErr": discoveryInfoErr,
	}).Info("Event received")
	return &s3Event, nil
}

func appendDynamicS3BucketLambda(api *sparta.API, lambdaFunctions []*sparta.LambdaAWSInfo) []*sparta.LambdaAWSInfo {
	svc := s3.New(session.New())
	svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String("hello"),
		Key:    aws.String("world"),
	})

	s3BucketResourceName := sparta.CloudFormationResourceName("S3DynamicBucket")
	lambdaFn := sparta.HandleAWSLambda(sparta.LambdaName(echoS3DynamicBucketEvent),
		echoS3DynamicBucketEvent,
		sparta.IAMRoleDefinition{})
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

	s3Decorator := func(serviceName string,
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
	lambdaFn.Decorators = []sparta.TemplateDecoratorHandler{
		sparta.TemplateDecoratorHookFunc(s3Decorator),
	}
	return append(lambdaFunctions, lambdaFn)
}

////////////////////////////////////////////////////////////////////////////////
// SNS handler
//
func echoSNSEvent(ctx context.Context, snsEvent awsLambdaEvents.SNSEvent) (*awsLambdaEvents.SNSEvent, error) {
	logger, _ := ctx.Value(sparta.ContextKeyRequestLogger).(*logrus.Entry)
	logger.WithFields(logrus.Fields{
		"Event": snsEvent,
	}).Info("Event received")
	return &snsEvent, nil
}

func appendSNSLambda(api *sparta.API, lambdaFunctions []*sparta.LambdaAWSInfo) []*sparta.LambdaAWSInfo {
	lambdaFn := sparta.HandleAWSLambda(sparta.LambdaName(echoSNSEvent),
		echoSNSEvent,
		sparta.IAMRoleDefinition{})
	lambdaFn.Permissions = append(lambdaFn.Permissions, sparta.SNSPermission{
		BasePermission: sparta.BasePermission{
			SourceArn: snsTopic,
		},
	})
	return append(lambdaFunctions, lambdaFn)
}

func echoDynamicSNSEvent(ctx context.Context, snsEvent awsLambdaEvents.SNSEvent) (*awsLambdaEvents.SNSEvent, error) {
	logger, _ := ctx.Value(sparta.ContextKeyRequestLogger).(*logrus.Entry)
	logger.WithFields(logrus.Fields{
		"Event": snsEvent,
	}).Info("Event received")
	return &snsEvent, nil
}

func appendDynamicSNSLambda(api *sparta.API, lambdaFunctions []*sparta.LambdaAWSInfo) []*sparta.LambdaAWSInfo {
	snsTopicName := sparta.CloudFormationResourceName("SNSDynamicTopic")
	lambdaFn := sparta.HandleAWSLambda(sparta.LambdaName(echoDynamicSNSEvent),
		echoDynamicSNSEvent,
		sparta.IAMRoleDefinition{})
	lambdaFn.Permissions = append(lambdaFn.Permissions, sparta.SNSPermission{
		BasePermission: sparta.BasePermission{
			SourceArn: gocf.Ref(snsTopicName),
		},
	})
	snsDecorator := func(serviceName string,
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
	lambdaFn.Decorators = []sparta.TemplateDecoratorHandler{
		sparta.TemplateDecoratorHookFunc(snsDecorator),
	}
	return append(lambdaFunctions, lambdaFn)
}

////////////////////////////////////////////////////////////////////////////////
// DynamoDB handler
//
func echoDynamoDBEvent(ctx context.Context, ddbEvent awsLambdaEvents.DynamoDBEvent) (*awsLambdaEvents.DynamoDBEvent, error) {
	logger, _ := ctx.Value(sparta.ContextKeyRequestLogger).(*logrus.Entry)
	logger.WithFields(logrus.Fields{
		"Event": ddbEvent,
	}).Info("Event received")
	return &ddbEvent, nil
}

func appendDynamoDBLambda(api *sparta.API, lambdaFunctions []*sparta.LambdaAWSInfo) []*sparta.LambdaAWSInfo {
	lambdaFn := sparta.HandleAWSLambda(sparta.LambdaName(echoDynamoDBEvent),
		echoDynamoDBEvent,
		sparta.IAMRoleDefinition{})
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
func echoKinesisEvent(ctx context.Context, kinesisEvent awsLambdaEvents.KinesisEvent) (*awsLambdaEvents.KinesisEvent, error) {
	logger, _ := ctx.Value(sparta.ContextKeyRequestLogger).(*logrus.Entry)
	logger.WithFields(logrus.Fields{
		"Event": kinesisEvent,
	}).Info("Event received")
	return &kinesisEvent, nil
}

func appendKinesisLambda(api *sparta.API, lambdaFunctions []*sparta.LambdaAWSInfo) []*sparta.LambdaAWSInfo {
	lambdaFn := sparta.HandleAWSLambda(sparta.LambdaName(echoKinesisEvent),
		echoKinesisEvent,
		sparta.IAMRoleDefinition{})
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
func echoSESEvent(ctx context.Context, sesEvent spartaSES.Event) (*spartaSES.Event, error) {
	logger, _ := ctx.Value(sparta.ContextKeyRequestLogger).(*logrus.Entry)
	configuration, configErr := sparta.Discover()
	logger.WithFields(logrus.Fields{
		"Error":         configErr,
		"Configuration": configuration,
	}).Info("Discovery results")

	// The message bucket is an explicit `DependsOn` relationship, so it'll be in the
	// resources map.  We'll find it by looking for the dependent resource with the "AWS::S3::Bucket" type
	bucketName := ""
	for _, eachResourceInfo := range configuration.Resources {
		if eachResourceInfo.ResourceType == "AWS::S3::Bucket" {
			bucketName = eachResourceInfo.Properties["Ref"]
		}
	}
	if "" == bucketName {
		return nil, errors.Errorf("Failed to discover SES bucket from sparta.Discovery: %#v", configuration)
	}
	// Get the metdata about the item...
	svc := s3.New(session.New())
	for _, eachRecord := range sesEvent.Records {
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
	return &sesEvent, nil
}

func appendSESLambda(api *sparta.API,
	lambdaFunctions []*sparta.LambdaAWSInfo) []*sparta.LambdaAWSInfo {

	// Our lambda function will need to be able to read from the bucket, which
	// will be handled by the S3MessageBodyBucketDecorator below
	lambdaFn := sparta.HandleAWSLambda(sparta.LambdaName(echoSESEvent),
		echoSESEvent,
		sparta.IAMRoleDefinition{})
	// Setup options s.t. the lambda function has time to consume the message body
	lambdaFn.Options = &sparta.LambdaFunctionOptions{
		Description: "",
		MemorySize:  128,
		Timeout:     10,
	}

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
func echoCloudWatchEvent(ctx context.Context, event map[string]interface{}) (map[string]interface{}, error) {
	logger, _ := ctx.Value(sparta.ContextKeyRequestLogger).(*logrus.Entry)

	logger.WithFields(logrus.Fields{
		"Event": event,
	}).Info("Request received")
	return event, nil
}
func appendCloudWatchEventHandler(api *sparta.API,
	lambdaFunctions []*sparta.LambdaAWSInfo) []*sparta.LambdaAWSInfo {

	lambdaFn := sparta.HandleAWSLambda(sparta.LambdaName(echoCloudWatchEvent),
		echoCloudWatchEvent,
		sparta.IAMRoleDefinition{})

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
func echoCloudWatchLogsEvent(ctx context.Context, cwlEvent awsLambdaEvents.CloudwatchLogsEvent) (*awsLambdaEvents.CloudwatchLogsEvent, error) {
	logger, _ := ctx.Value(sparta.ContextKeyRequestLogger).(*logrus.Entry)

	logger.WithFields(logrus.Fields{
		"Event": cwlEvent,
	}).Info("Request received")
	return &cwlEvent, nil
}

func appendCloudWatchLogsHandler(api *sparta.API,
	lambdaFunctions []*sparta.LambdaAWSInfo) []*sparta.LambdaAWSInfo {

	lambdaFn := sparta.HandleAWSLambda(sparta.LambdaName(echoCloudWatchLogsEvent),
		echoCloudWatchLogsEvent,
		sparta.IAMRoleDefinition{})
	cloudWatchLogsPermission := sparta.CloudWatchLogsPermission{}
	cloudWatchLogsPermission.Filters = make(map[string]sparta.CloudWatchLogsSubscriptionFilter, 1)
	cloudWatchLogsPermission.Filters["MyFilter"] = sparta.CloudWatchLogsSubscriptionFilter{
		FilterPattern: "",
		LogGroupName:  "/sparta/testing",
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

	awsSession := session.New()
	stackName, stackNameErr := spartaCF.UserAccountScopedStackName("SpartaApplication", awsSession)
	if stackNameErr != nil {
		os.Exit(1)
	}
	sparta.Main(stackName,
		"Simple Sparta application",
		spartaLambdaData(apiGateway),
		apiGateway,
		nil)
}
