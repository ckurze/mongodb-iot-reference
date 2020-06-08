# Setup IoT Demo on Amazon Web Services
This demo shows you how to deploy the IoT reference implementation on AWS. The following instructions assume that you already have basic knowledge of working with AWS and will not cover all of the basics.

## Architecture Overview
The MongoDB AWS IoT reference implementation can be divided into two logical sections. First, the public IoT data is retrieved from the Citibike API and then written to IoT Core. This is done event-driven using a Lambda Function. This part of the implementation can be seen as a simulation of physical IoT devices. In a real world implementation this data would be written directly from edge devices to IoT Core using standard protocols such as MQTT or HTTPS.
![](implementation_overview.png)

The second part of the implementation includes the actual processing of the IoT data. This section starts with IoT Core and ends in the MongoDB. The IoT Core component provides endpoints, device management and routing mechanisms. Another Lambda Function reads the data from IoT Core, processes it and finally writes it to the MongoDB.

## Prerequisites
* MongoDB Atlas Cluster*
* existing AWS Account
* [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html) installed and logged in
* The visualization in MongoDB Charts is explained in the [overview readme](../README.md#visualization)

\*Make sure that the Atlas cluster can be accessed from the Google Cloud Function. For demo purposes the network access can be set to 0.0.0.0/0 (In MongoDB Atlas). This allows access from any IP. However, this setting is not recommended for production environments.
  
## Create Cloud Resources
The demo requires the following AWS components, which we will create step by step:

* A role to be used by the Lambda Functions/AWS IoT Core
* Lambda Function: device_simulation
* Trigger to call device_simulation every 10 minutes
* Lambda Function: iotcore_to_mongodb
* A rule to trigger the function iotcore_to_mongodb whenever a device gets an update
* Initialization of indexes and views in MongoDB
  

### Create a new Role in AWS
First we will create a new role, that can execute Lambda Function and access the IoT Core stack.

1. Navigate to "IAM" > "Roles"
2. Create a new role
3. Assign the following policies to that new role: `AWSLambdaBasicExecutionRole` & `AWSIoTFullAccess`


### Create Lambda Function: device_simulation

Navigate to `/setup` and execute the following command - replace the `role arn` with the one you created: 

```
aws lambda create-function \
--function-name device_simulation \
--runtime python3.6 \
--role arn:aws:iam::928549030570:role/IotDemoRole \
--handler app.lambda_handler \
--zip-file fileb://device_simulation.zip --timeout 240
```

AWS CLI Response:

```
{
    "LastUpdateStatus": "Successful", 
    "FunctionName": "device_simulation", 
    "LastModified": "2020-06-03T19:00:39.381+0000", 
    "RevisionId": "064baa74-54ff-4dbb-927d-50b41215daf6", 
    "MemorySize": 128, 
    "State": "Active", 
    "Version": "$LATEST", 
    "Role": "arn:aws:iam::928549030570:role/IotDemoRole", 
    "Timeout": 240, 
    "Runtime": "python3.6", 
    "TracingConfig": {
        "Mode": "PassThrough"
    }, 
    "CodeSha256": "hjQd/osEfDN60BK0ox+6zPrVWZIYlmiRJHcNB7hh/p4=", 
    "Description": "", 
    "CodeSize": 6391438, 
    "FunctionArn": "arn:aws:lambda:eu-central-1:928549030570:function:device_simulation", 
    "Handler": "app.lambda_handler"
}
```


### Add trigger for device_simulation

Create a rule to trigger the device_simulation Lambda Function every 10 minutes:

```
aws events put-rule --name "refresh_deviceSimulation" \
--schedule-expression "rate(10 minutes)"
```

AWS CLI Respone:

```
{
    "RuleArn": "arn:aws:events:eu-central-1:928549030570:rule/refresh_deviceSimulation"
}
```
Grant permission to the rule to allow it to trigger the device_simulation function - replace the `source-arn` with the one from the CLI output of the previous part.
```
aws lambda add-permission --function-name device_simulation \
--statement-id 123 \
--action 'lambda:InvokeFunction' \
--principal events.amazonaws.com \
--source-arn arn:aws:events:eu-central-1:928549030570:rule/refresh_deviceSimulation
```

Add the `ARN` of your device_simulation Lambda Function as a target for the newly created rule:
```
aws events put-targets --rule "refresh_deviceSimulation" --targets "Id"="1","Arn"="arn:aws:lambda:eu-central-1:928549030570:function:device_simulation"
```

AWS CLI Respone:
```
{
    "FailedEntries": [], 
    "FailedEntryCount": 0
}
```

### Create Lambda Function: iotcore_to_mongodb

Navigate to `/setup` and replace `XYZ` with your MongoDB Atlas URI and `role arn` with the one you created during the first step. After that, execute the command.

```
aws lambda create-function \
--function-name iotcore_to_mongodb \
--runtime python3.6 \
--role arn:aws:iam::928549030570:role/IotDemoRole \
--handler app.lambda_handler \
--zip-file fileb://iotcore_to_mongodb.zip \
--timeout 240 \
--environment Variables={MONGO_URI="XYZ"}
```
AWS CLI Respone:
```
{
    "LastUpdateStatus": "Successful", 
    "FunctionName": "iotcore_to_mongodb", 
    "LastModified": "2020-06-03T19:07:56.114+0000", 
    "RevisionId": "9323ad8a-9d8b-49cd-b280-13cea736614b", 
    "MemorySize": 128, 
    "Environment": {
        "Variables": {
            "MONGO_URI": "XYZ"
        }
    }, 
    "State": "Active", 
    "Version": "$LATEST", 
    "Role": "arn:aws:iam::928549030570:role/IotDemoRole", 
    "Timeout": 240, 
    "Runtime": "python3.6", 
    "TracingConfig": {
        "Mode": "PassThrough"
    }, 
    "CodeSha256": "132bBG8natdcbT+R9Gs34oGw2j+fcdWLYbX9DPnMrg4=", 
    "Description": "", 
    "CodeSize": 4445612, 
    "FunctionArn": "arn:aws:lambda:eu-central-1:928549030570:function:iotcore_to_mongodb", 
    "Handler": "app.lambda_handler"
}
```

### Create a rule for iotcore_to_mongodb

The last step is to create a rule that calls the iotcore_to_mongodb Lambda Function, every time the shadow of a thing gets updated.

Navigate to `/setup` and open the file `rule.json`. Exchange the `functionARN` with the ARN from your iotcore_to_mongod Lambda Function:

```
{
  "sql": "SELECT * FROM '$aws/things/+/shadow/update/accepted'",
  "ruleDisabled": false,
  "awsIotSqlVersion": "2016-03-23",
  "actions": [{
      "lambda": {
          "functionArn": "arn:aws:lambda:eu-central-1:928549030570:function:iotcore_to_mongodb"
      }
  }]
}
```
Allow AWS IoT to call the iotcore_to_mongodb Lambda Function:
```
aws lambda add-permission --function-name "iotcore_to_mongodb" --statement-id 123 --action "lambda:InvokeFunction" --principal iot.amazonaws.com
```

Create the new rule using the following command:
```
aws iot create-topic-rule --rule-name writeMongoDB --topic-rule-payload file://rule.json
```

### Initialize MongoDB
These initialization steps could also go into the startup phases of other components. Please execute this script once to initialize the proper indexes and views in MongoDB:

```
export MONGO_URI="mongodb+srv://ATLAS_USERNAME:ATLAS_PASSWORD@iot-demo-XXXXX.mongodb.net/test?retryWrites=true&w=majority"
python3 initialize_mongodb.py
```
