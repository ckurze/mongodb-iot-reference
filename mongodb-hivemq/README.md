# Citibike IoT Reference Implementation: MongoDB & HiveMQ

This demo sets up an MQTT Publisher written in Python that sends data frequently to status topics in HiveMQ. These are read by another Pyhton based subscriber batching operations to MongoDB.

## Architecture Overview

![](implementation-overview.png)

## Environment Setup

### Devices
No setup for the devices is needed. Access to the [Citibike GBFS Feed](https://www.citibikenyc.com/system-data) is available free of charge and can be accessed via their [public API endpoint](http://gbfs.citibikenyc.com/gbfs/gbfs.json).

### MongoDB Atlas Cluster (Free Tier)
Please follow the instructions in the [MongoDB Atlas Getting Started Guide](https://docs.atlas.mongodb.com/getting-started/) and spin up a free tier cluster. **Please make sure to use MongoDB version 4.2 or newer**. 

You are required to whitelist the IP address of the MQTT subcribers (see below), otherwise you cannot access the data in your cluster from the MQTT subscribers. More advanced options like VPC peering or AWS Private Link are also available (see the [docs](https://docs.atlas.mongodb.com/setup-cluster-security/) for more details).

### HiveMQ MQTT Broker
Please set up an HiveMQ Broker. A fully managed service is available in the [HiveMQ Cloud](https://www.hivemq.com/cloud/). As an alternative, [pre-configured HiveMQ AWS images](https://www.hivemq.com/downloads/aws/) are available which allow to set up a HiveMQ Broker within a couple of minutes.

### Gateway 
As the gateway, we are going to use a t2.micro instance on AWS which frequently executes python scripts to read the citibike data feed and put the data into an MQTT topic.

As a preparation, please execute the following commands on a fresh instance (assuming Amazon Linux 2):
```
sudo yum -y update
sudo yum -y install python3 git
sudo pip3 install pymongo dnspython requests paho-mqtt
```

Create a `.cron_profile` file in the home directory. Please replace user and password with the values you created above. Also use the connection string from the step [Connect to Your Cluster](https://docs.atlas.mongodb.com/tutorial/connect-to-your-cluster/) of the Atlas Getting Started Guide. The MQTT_HOST is either the publicly available IP or hostname of your EC2 instance or the provided connection by HiveMQ Cloud.
```
export MONGO_URI="mongodb+srv://ATLAS_USERNAME:ATLAS_PASSWORD@iot-demo-XXXXX.mongodb.net/test?retryWrites=true&w=majority"
export MQTT_HOST="ec2-XXX-XXX-XXX-XXX.REGION.compute.amazonaws.com"
```

Copy the implemenation files (including all subdirectories) to your home directory for better convenience:
```
cd ~
cp -r PATH_TO_CLONED_GITHUB_REPO/monogdb-hivemq/* .
```

Add the processes to the crontab via `crontab -e` and add the following commands:
```
# We leverage crontab for a frequent execution of the import from the data feeds.
# The log files will contain the results of the latest execution for debugging purposes.

# Refresh stations every hour
0 * * * * . $HOME/.cron_profile; $HOME/station_information_publish.sh > station_information_publish.log

# Refresh status every 30 seconds
* * * * * . $HOME/.cron_profile; $HOME/runEvery.sh 30 "$HOME/station_status_publish.sh" > station_status_publish.log 2>&1
```

### MQTT Subsribers
For the sake of simplicity, we execute the MQTT subsribers on the same machine as the gateway. 

As all environment variables are already configured in `.cron_profile`, we only need to start the subscribers via:
```
cd ~
source .cron_profile
nohup python3 station_information_subscribe.py > station_information_subscribe.log 2>&1 &
nohup python3 station_status_subscribe.py > station_status_subscribe.log 2>&1 &
```

### Initialize the Setup on the Gateway
All necessary indexes and views will be created automatically by the MQTT subsribers. As the station information will be read on an hourly basis only, please execute the following command once during the setup process. *Important* - the subcribers have to be started in advance:
```
cd ~
source .cron_profile
./station_information_publish.sh
```

