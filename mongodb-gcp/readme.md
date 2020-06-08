# Setup IoT Demo on Google Cloud Platform
This demo shows you how to deploy the IoT reference implementation on Google Cloud Platform. The following instructions assume that you already have basic knowledge of working with GCP and will not cover all of the basics.

## Architecture Overview
The MongoDB Azure IoT reference implementation can be divided into two logical sections. First, the public IoT data is retrieved from the Citibike API and then written to IoT Core. This is done event-driven using an Google Cloud Function. This part of the implementation can be seen as a simulation of physical IoT devices. In a real world implementation this data would be written directly from edge devices to IoT Core using standard protocols such as MQTT or HTTPS.
![](images/implementation_overview.png)

The second part of the implementation includes the actual processing of the IoT data. This section starts with IoT Core and ends in the MongoDB. The IoT Core component provides endpoints, device management and routing mechanisms. Another Google Cloud Function reads the data from IoT Core (through Pub/Sub), processes it and finally writes it to the MongoDB.

## Prerequisites
* MongoDB Atlas Cluster*
* existing GCP Account
* [GCP Cloud SDK](https://cloud.google.com/sdk) installed and logged in
* The visualization in MongoDB Charts is explained in the [overview readme](../README.md#visualization)

\*Make sure that the Atlas cluster can be accessed from the Google Cloud Function. For demo purposes the network access can be set to 0.0.0.0/0 (In MongoDB Atlas). This allows access from any IP. However, this setting is not recommended for production environments.
  
## Create Cloud Resources
The demo requires the following GCP resources, which we will create step by step:

* new project (optional)
* IoT Core 
* Pub/Sub
* Cloud Function: device_simulation
* Cloud Function: iotcore_to_mongodb
* Cloud Scheduler
* Initialization of indexes and views in MongoDB
  
### Define variables 
The easiest way to do this tutorial is to first define a few variables
```
project_id="iot-mongodb-demo"
region="europe-west1"
device_registry="citibikes"
topic_name="device_messages"
```

### Create a new Google Cloud Project (optional)
First we create a new project, but you can also use an existing project.

```
gcloud projects create $project_id --name=$project_id
gcloud config set project $project_id
``` 
The project must be linked to a billing account. Currently, linking projects to a billing account is only available in a beta version of the CLI, so it is recommended to do this in the [Google Cloud Console](https://console.cloud.google.com).

### Create new registry in IoT Core with topic
[Google Cloud Internet of Things (IoT) Core](https://cloud.google.com/iot/docs) is a fully managed service for securely connecting and managing IoT devices, from a few to millions. We will use it to retrieve data from our simulated devices. In a production enviroment the edge devices would send data directly to IoT Core. The IoT Core component needs a topic to forward the messages. First you can create a new Pub/Sub topic with this command:
```
gcloud pubsub topics create $topic_name
```

Then create a new registry in IoT Core. (You will get asked a few times during this tutorial to active APIs. Do that when you're asked.): 
```
gcloud iot registries create --region=$region $device_registry --event-notification-config=topic=$topic_name
```

### Create Google Cloud Functions
[Cloud Functions](https://cloud.google.com/functions/) is Google Cloud’s event-driven serverless compute platform. We will have two seperated functions, one that simulate the iot devices and one that work as a bridge between IoT Core and the MongoDB.

#### Device Simulation
Navigate to `mongodb-gcp/device_simulation` and update the settings.yml file. Set `IOT_PROJECT_ID`, `IOT_REGION` and `IOT_REGISTRY_ID` according to the already defined variables. Deploy the `device_simulation` function as new cloud function:
```
gcloud functions deploy device_simulation --trigger-http --allow-unauthenticated --runtime python37 --region $region --memory 256 --timeout 300 --env-vars-file settings.yml --entry-point refresh
```
#### IoT Core to MongoDB
Navigate to `mongodb-gcp/iotcore_to_mongodb` and update the settings.yml again. Set `MONGO_URI` to a python driver compatible connection string of the previously created MongoDB Atlas cluster.
```
gcloud functions deploy iotcore_to_mongodb --trigger-topic $topic_name --allow-unauthenticated --runtime python37 --region $region --memory 256 --timeout 300 --env-vars-file settings.yml --entry-point process
```
In both examples we publish the settings.yml as environment variables in Cloud Functions. 


### Create Cloud Scheduler for Function Invocation
The Cloud Scheduler will call the corresponding Cloud Function url periodically. To setup the Cloud Scheduler you must first retrieve the configuration of the __device simulation__ function:
```
gcloud functions describe device_simulation --region $region --format json
```
Copy the value of "httpsTrigger" and use it to setup the Cloud Scheduler:
```
gcloud scheduler jobs create http update_iot --schedule "*/10 * * * *" --uri "ValueOfHttpsTrigger" --http-method POST --time-zone Europe/Berlin
```

The Cloud Scheduler will trigger the device_simulation every 10 minutes. This will write all the messages to IoT Core, which will publish them in the created Pub/Sub topic. The second Cloud Function (iotcore_to_mongodb) will then retrieve the messages and will write them to the MongoDB.

### Initialize MongoDB
These initialization steps could also go into the startup phases of other components. Please execute this script once to initialize the proper indexes and views in MongoDB:

```
export MONGO_URI="mongodb+srv://ATLAS_USERNAME:ATLAS_PASSWORD@iot-demo-XXXXX.mongodb.net/test?retryWrites=true&w=majority"
python3 initialize_mongodb.py
```

