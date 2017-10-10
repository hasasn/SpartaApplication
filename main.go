package main

import (
	"encoding/json"
	"io/ioutil"
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

var s3Bucket = paramVal("SPARTA_S3_TEST_BUCKET", "arn:aws:s3:::MyS3Bucket")
var snsTopic = paramVal("SPARTA_SNS_TEST_TOPIC", "arn:aws:sns:us-west-2:123412341234:mySNSTopic")
var dynamoTestStream = paramVal("SPARTA_DYNAMO_TEST_STREAM", "arn:aws:dynamodb:us-west-2:123412341234:table/myTableName/stream/2015-10-22T15:05:13.779")
var kinesisTestStream = paramVal("SPARTA_KINESIS_TEST_STREAM", "arn:aws:kinesis:us-west-2:123412341234:stream/kinesisTestStream")

////////////////////////////////////////////////////////////////////////////////
// S3 handler
//
func echoS3Event(w http.ResponseWriter, r *http.Request) {
	logger, _ := r.Context().Value(sparta.ContextKeyLogger).(*logrus.Logger)
	lambdaContext, _ := r.Context().Value(sparta.ContextKeyLambdaContext).(*sparta.LambdaContext)

	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()
	var jsonMessage json.RawMessage
	err := decoder.Decode(&jsonMessage)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	logger.WithFields(logrus.Fields{
		"RequestID": lambdaContext.AWSRequestID,
		"Event":     string(jsonMessage),
	}).Info("Request received")

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonMessage)
}

func appendS3Lambda(api *sparta.API, lambdaFunctions []*sparta.LambdaAWSInfo) []*sparta.LambdaAWSInfo {
	lambdaFn := sparta.HandleAWSLambda(sparta.LambdaName(echoS3Event),
		http.HandlerFunc(echoS3Event),
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

func echoS3DynamicBucketEvent(w http.ResponseWriter, r *http.Request) {
	logger, _ := r.Context().Value(sparta.ContextKeyLogger).(*logrus.Logger)
	lambdaContext, _ := r.Context().Value(sparta.ContextKeyLambdaContext).(*sparta.LambdaContext)
	eventData, _ := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	config, _ := sparta.Discover()
	logger.WithFields(logrus.Fields{
		"RequestID":     lambdaContext.AWSRequestID,
		"Event":         string(eventData),
		"Configuration": config,
	}).Info("Request received")

	w.Header().Set("Content-Type", "application/json")
	w.Write(eventData)
}

func appendDynamicS3BucketLambda(api *sparta.API, lambdaFunctions []*sparta.LambdaAWSInfo) []*sparta.LambdaAWSInfo {

	s3BucketResourceName := sparta.CloudFormationResourceName("S3DynamicBucket")
	lambdaFn := sparta.HandleAWSLambda(sparta.LambdaName(echoS3DynamicBucketEvent),
		http.HandlerFunc(echoS3DynamicBucketEvent),
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
func echoSNSEvent(w http.ResponseWriter, r *http.Request) {
	logger, _ := r.Context().Value(sparta.ContextKeyLogger).(*logrus.Logger)
	lambdaContext, _ := r.Context().Value(sparta.ContextKeyLambdaContext).(*sparta.LambdaContext)

	logger.WithFields(logrus.Fields{
		"RequestID": lambdaContext.AWSRequestID,
	}).Info("Request received")

	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()
	var lambdaEvent spartaSNS.Event
	err := decoder.Decode(&lambdaEvent)
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
	lambdaFn := sparta.HandleAWSLambda(sparta.LambdaName(echoSNSEvent),
		http.HandlerFunc(echoSNSEvent),
		sparta.IAMRoleDefinition{})
	lambdaFn.Permissions = append(lambdaFn.Permissions, sparta.SNSPermission{
		BasePermission: sparta.BasePermission{
			SourceArn: snsTopic,
		},
	})
	return append(lambdaFunctions, lambdaFn)
}

func echoDynamicSNSEvent(w http.ResponseWriter, r *http.Request) {

	logger, _ := r.Context().Value(sparta.ContextKeyLogger).(*logrus.Logger)
	lambdaContext, _ := r.Context().Value(sparta.ContextKeyLambdaContext).(*sparta.LambdaContext)
	logger.WithFields(logrus.Fields{
		"RequestID": lambdaContext.AWSRequestID,
	}).Info("Request received")

	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()
	var lambdaEvent spartaSNS.Event
	err := decoder.Decode(&lambdaEvent)
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
	lambdaFn := sparta.HandleAWSLambda(sparta.LambdaName(echoDynamicSNSEvent),
		http.HandlerFunc(echoDynamicSNSEvent),
		sparta.IAMRoleDefinition{})
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
func echoDynamoDBEvent(w http.ResponseWriter, r *http.Request) {
	logger, _ := r.Context().Value(sparta.ContextKeyLogger).(*logrus.Logger)
	lambdaContext, _ := r.Context().Value(sparta.ContextKeyLambdaContext).(*sparta.LambdaContext)
	logger.WithFields(logrus.Fields{
		"RequestID": lambdaContext.AWSRequestID,
	}).Info("Request received")

	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()
	var lambdaEvent spartaDynamoDB.Event
	err := decoder.Decode(&lambdaEvent)
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
	bytes, bytesErr := json.Marshal(&lambdaEvent)
	if bytes != nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write(bytes)
	} else {
		http.Error(w, bytesErr.Error(), http.StatusInternalServerError)
	}
}

func appendDynamoDBLambda(api *sparta.API, lambdaFunctions []*sparta.LambdaAWSInfo) []*sparta.LambdaAWSInfo {
	lambdaFn := sparta.HandleAWSLambda(sparta.LambdaName(echoDynamoDBEvent),
		http.HandlerFunc(echoDynamoDBEvent),
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
func echoKinesisEvent(w http.ResponseWriter, r *http.Request) {
	logger, _ := r.Context().Value(sparta.ContextKeyLogger).(*logrus.Logger)
	lambdaContext, _ := r.Context().Value(sparta.ContextKeyLambdaContext).(*sparta.LambdaContext)
	logger.WithFields(logrus.Fields{
		"RequestID": lambdaContext.AWSRequestID,
	}).Info("Request received")

	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()
	var lambdaEvent spartaKinesis.Event
	err := decoder.Decode(&lambdaEvent)
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
	lambdaFn := sparta.HandleAWSLambda(sparta.LambdaName(echoKinesisEvent),
		http.HandlerFunc(echoKinesisEvent),
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
func echoSESEvent(w http.ResponseWriter, r *http.Request) {
	logger, _ := r.Context().Value(sparta.ContextKeyLogger).(*logrus.Logger)
	lambdaContext, _ := r.Context().Value(sparta.ContextKeyLambdaContext).(*sparta.LambdaContext)
	logger.WithFields(logrus.Fields{
		"RequestID": lambdaContext.AWSRequestID,
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

	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()
	var lambdaEvent spartaSES.Event
	err := decoder.Decode(&lambdaEvent)
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

	// Our lambda function will need to be able to read from the bucket, which
	// will be handled by the S3MessageBodyBucketDecorator below
	lambdaFn := sparta.HandleAWSLambda(sparta.LambdaName(echoSESEvent),
		http.HandlerFunc(echoSESEvent),
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
func echoCloudWatchEvent(w http.ResponseWriter, r *http.Request) {
	logger, _ := r.Context().Value(sparta.ContextKeyLogger).(*logrus.Logger)
	lambdaContext, _ := r.Context().Value(sparta.ContextKeyLambdaContext).(*sparta.LambdaContext)
	logger.WithFields(logrus.Fields{
		"RequestID": lambdaContext.AWSRequestID,
	}).Info("Request received")

	config, _ := sparta.Discover()
	logger.WithFields(logrus.Fields{
		"RequestID":     lambdaContext.AWSRequestID,
		"Configuration": config,
	}).Info("Request received")

	w.Write([]byte("CloudWatch event received!"))
}
func appendCloudWatchEventHandler(api *sparta.API,
	lambdaFunctions []*sparta.LambdaAWSInfo) []*sparta.LambdaAWSInfo {

	lambdaFn := sparta.HandleAWSLambda(sparta.LambdaName(echoCloudWatchEvent),
		http.HandlerFunc(echoCloudWatchEvent),
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
func echoCloudWatchLogsEvent(w http.ResponseWriter, r *http.Request) {
	logger, _ := r.Context().Value(sparta.ContextKeyLogger).(*logrus.Logger)
	lambdaContext, _ := r.Context().Value(sparta.ContextKeyLambdaContext).(*sparta.LambdaContext)
	logger.WithFields(logrus.Fields{
		"RequestID": lambdaContext.AWSRequestID,
	}).Info("Request received")

	w.Write([]byte("CloudWatch event received!"))
}

func appendCloudWatchLogsHandler(api *sparta.API,
	lambdaFunctions []*sparta.LambdaAWSInfo) []*sparta.LambdaAWSInfo {

	lambdaFn := sparta.HandleAWSLambda(sparta.LambdaName(echoCloudWatchLogsEvent),
		http.HandlerFunc(echoCloudWatchLogsEvent),
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

	stackName := spartaCF.UserScopedStackName("SpartaApplication")
	sparta.Main(stackName,
		"Simple Sparta application",
		spartaLambdaData(apiGateway),
		apiGateway,
		nil)
}
