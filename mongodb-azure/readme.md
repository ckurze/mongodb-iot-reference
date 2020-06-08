# Setup IoT Demo on Microsoft Azure
This demo shows you how to deploy the IoT reference implementation on Microsoft Azure. The following instructions assume that you already have basic knowledge of working with Microsoft Azure and will not cover all of the basics.

## Architecture Overview
The MongoDB Azure IoT reference implementation can be divided into two logical sections. First, the public IoT data is retrieved from the Citibike API and then written to the Azure IoT Hub. This is done event-driven using an Azure Function. This part of the implementation can be seen as a simulation of physical IoT devices. In a real world implementation this data would be written directly from edge devices to the Azure IoT Hub using the Azure IoT SDK or a standard protocol such as MQTT.
![](images/implementation_overview.png)

The second part of the implementation includes the actual processing of the IoT data. This section starts with the IoT Hub and ends in the MongoDB. The IoT Hub provides endpoints, device management and routing mechanisms. Another Azure function reads the data from the IoT Hub, processes it and finally writes it to the MongoDB. 

## Prerequisites
* MongoDB Atlas Cluster*
* existing Azure Account/Tenant
* [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest) installed and linked to active subscription (should work with [Azure Cloud Shell](https://shell.azure.com/), too)
* [Azure Functions Core Tools](https://docs.microsoft.com/en-us/azure/azure-functions/functions-run-local) installed (newest version)
* The visualization in MongoDB Charts is explained in the [overview readme](../README.md#visualization)
  
\*Make sure that the Atlas cluster can be accessed from the Azure Function. For demo purposes the network access can be set to 0.0.0.0/0 (In MongoDB Atlas). This allows access from any IP. However, this setting is not recommended for production environments. 

## Create Azure Resources
The demo requires the following Azure resources, which we will create step by step:

* Resource Group: mongodb-iot-ref-impl 
* IoT Hub: iot-hub-mongodb-xxxxx
* Azure Function App: iot-device-simulation-xxxxx (including a dedicated storage account)
* Azure Function App: iothub-to-mongodb-xxxxx (including a dedicated storage account)
* Initialization of indexes and views in MongoDB
  
### Define variables 
The easiest way to follow this tutorial is to first define a few variables. I created the resources through the PowerShell version of the Azure Cloud Shell. Depending on the shell you use the syntax of the variable definition can vary. A few resoruces need unique names (across all tenants not only yours!), so adding a random suffix may be a good idea. Please notice that depending on the resource type additional naming restriction apply. For example, storage accounts in azure must not include any whitespaces or special characters.

```
$yourLocation = 'westeurope'
$resource_grp_name = 'mongodb-iot-ref-impl-demo'
$iot_hub_name = 'iot-hub-mongo-xxxxx'
$device_simulation_app_name = 'iot-device-simulation-xxxxx'
$device_storage_acc_name = 'iotdevsimstoragexxxxx'
$iothub_to_mongodb_app_name = 'iothub-to-mongodb-xxxxx'
$tomongodb_storage_acc_name = 'tomongostoragexxxxx'
```

### Create a new resource group
A resource group is a container that holds related resources for an Azure solution ([more information](https://docs.microsoft.com/en-us/azure/azure-resource-manager/management/manage-resource-groups-portal)). 

With the following command you can create a resource group that we will use for your demo IoT solution. To get a list of all available Azure regions/locations run `az account list-locations`.

```
az group create -l $yourLocation -n $resource_grp_name
```

### Create the IoT Hub
The [Azure IoT Hub](https://docs.microsoft.com/en-us/azure/iot-hub/about-iot-hub) is a managed service to enable bi-directional communication between IoT devices and Azure. You can use the IoT Hub SDK or integrate your edge device by utilizing standard protocols such as MQTT. The free tier (F1) has a very restricted quota. To run the IoT demo application all day you have to choose a paid tier. Note that a free tier can not be upgraded later. Upgrades are available for paid tiers only. 

In order to be able to configure the Azure Functions correctly during the first deployment, we create the IoT Hub first.

```
az iot hub create --resource-group $resource_grp_name --name $iot_hub_name --sku F1 --partition-count 2
```

### Create Azure Function Apps
[Azure Functions](https://docs.microsoft.com/en-us/azure/azure-functions/) is a serverless compute service that lets you run event-triggered code without having to explicitly provision or manage infrastructure. We will have two seperated functions, one that simulate the iot devices and one that work as a bridge between the IoT Hub and the MongoDB. For each function we will deploy its own Function App. A Azure Function App is a group of related Azure Functions that is always deployed together. To seperate the simulation part and the actual IoT implementation we deploy two apps.

#### Device Simulation
Each Azure Function App will need its storage account. The storage account is used to store our application code. The following command creates the storage account:

```
az storage account create --name $device_storage_acc_name --location $yourLocation --resource-group $resource_grp_name --sku Standard_LRS
```

After the storage account is created, the Function App can be created. This command will not publish any functionality yet: 

```
az functionapp create --resource-group $resource_grp_name --consumption-plan-location $yourLocation --runtime python --runtime-version 3.7 --os-type Linux --functions-version 2 --name $device_simulation_app_name --storage-account $device_storage_acc_name
```

Then deploy the application code to Azure Function:
* Navigate to `mongodb-azure/device_simulation` of this repository. 
* rename or copy `local.settings.template.json` to `local.settings.json`
* Set `IOT_HUB_CONNECTION_STRING` in `local.settings.json`. You can retrieve the value by running `az iot hub show-connection-string --resource-group $resource_grp_name --name $iot_hub_name` 
* Set `IOT_HUB_HOSTNAME` in `local.settings.json`. You can extract the value from the previously retrieved connection string. The value can be found in the connection string behind `HostName`.

After you saved the `local.settings.json` file the function can be published including this configuration:

```
func azure functionapp publish $device_simulation_app_name --publish-local-settings
```
After the publishing is completed the function will be executed periodically and will write data from the Citibike API to the Azure IoT Hub. The timer is set through a property inside the Azure Function. If you want to change this value then change the CRON expression in `device_simulation/refresh/function.json` and publish it again.

#### IoT Hub to MongoDB
A storage account is also required for the second Azure Function:
```
az storage account create --name $tomongodb_storage_acc_name --location $yourLocation --resource-group $resource_grp_name --sku Standard_LRS
```

After the storage account is created, the Function App can be created: 

```
az functionapp create --resource-group $resource_grp_name --consumption-plan-location $yourLocation --runtime python --runtime-version 3.7 --os-type Linux --functions-version 2 --name $iothub_to_mongodb_app_name --storage-account $tomongodb_storage_acc_name
```

Then deploy the application code to Azure Function:

* Navigate to  `mongodb-azure/iothub_to_mongodb`. 
* Rename or copy `local.settings.template.json` to `local.settings.json`
* Set `EVENTHUB_CONNECTION_STRING` in `local.settings.json`
  * Important: This is not the connection string that was previously used in the device simulation Azure Function. To act on events in the IoT Hub we use a Azure Function that listens on new messages. The function will interprete the IoT Hub as a more generic Azure Event Hub. To use the IoT Hub as a Event Hub you must build a Event Hub compatible connection string.
    * First, retrieve the IoT Hub Configuration: `az iot hub show --name $iot_hub_name` 
    * Locate the values for `properties.eventHubEndpoints.events.endpoint` and `properties.eventHubEndpoints.events.path`
    * Build the new Event Hub compatible connection string with the following syntax: `Endpoint=EVENTHUBENDPOINT;SharedAccessKeyName=iothubowner;SharedAccessKey=SHAREDKEY;EntityPath=PATH`. Set the value for Endpoint and EntityPath to the just retrieved values. Fill the SharedKey from the IoT Hub connection string. An alternative way to get the connection string (and maybe the easier one here) is to retrieve it directly from the [Azure Portal](https://portal.azure.com). Navigate to the created IoT Hub and retrieve the connection string at `Build-in endpoints\Event Hub-compatible endpoint`.
* Set `MONGO_URI` in `local.settings.json`. This must be a python driver compatible MongoDB connection string of your Atlas Cluster
* Modify the `process/function.json`. Set `eventHubName` to the `PATH`/`EntityPath` value of your Event Hub connectino string.

After you saved the changed files, you can deploy the `iothub_to_mongodb` function as well:
```
func azure functionapp publish $iothub_to_mongodb_app_name --publish-local-settings
```

Now, `iothub_to_mongodb` will read the incoming messages at the IoT Hub and will insert them into the MongoDB Atlas cluster.

### Initialize MongoDB
These initialization steps could also go into the startup phases of other components. Please execute this script once to initialize the proper indexes and views in MongoDB:

```
export MONGO_URI="mongodb+srv://ATLAS_USERNAME:ATLAS_PASSWORD@iot-demo-XXXXX.mongodb.net/test?retryWrites=true&w=majority"
python3 initialize_mongodb.py
```
