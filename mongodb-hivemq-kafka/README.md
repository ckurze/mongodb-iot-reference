# Citibike IoT Reference Implementation: MongoDB & HiveMQ & Kafka

This demo sets up an MQTT Publisher written in Python that sends data frequently to status topics in HiveMQ. These are read by another Pyhton based subscriber batching operations to MongoDB.

## Architecture Overview

![](implementation_overview.png)

## Environment Setup

### Devices
No setup for the devices is needed. Access to the [Citibike GBFS Feed](https://www.citibikenyc.com/system-data) is available free of charge and can be accessed via their [public API endpoint](http://gbfs.citibikenyc.com/gbfs/gbfs.json).

### MongoDB Atlas Cluster (Free Tier)
Please follow the instructions in the [MongoDB Atlas Getting Started Guide](https://docs.atlas.mongodb.com/getting-started/) and spin up a free tier cluster. **Please make sure to use MongoDB version 4.2 or newer**. 

You are required to whitelist the IP address of the Kafka cluster (see below), otherwise you cannot access the data in your cluster from Kakfa. More advanced options like VPC peering or AWS Private Link are also available (see the [docs](https://docs.atlas.mongodb.com/setup-cluster-security/) for more details).

### Kafka and HiveMQ MQTT Broker

#### Preparation of the machine
We use AWS for this demo, but it can run anyhwere else. Please follow these steps on an AWS Linux 2 image to prepare the environment:
```bash
# Steps on Amazon Linux 2

sudo yum -y update
sudo yum install -y docker git maven python3
sudo pip3 install pymongo dnspython requests paho-mqtt confluent-kafka
sudo chkconfig docker on
sudo service docker start
sudo usermod -a -G docker ec2-user

sudo reboot

# check docker commands can be run as non-sudo
docker info

# Download latest docker compose
sudo curl -L "https://github.com/docker/compose/releases/download/1.25.5/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Verify it works
docker-compose --version
```

#### Start Confluent Kafka & HiveMQ Broker
For this implementation, we use a dockerized environment of Confluent Kafka and HiveMQ. Fully managed service are is available in the [HiveMQ Cloud](https://www.hivemq.com/cloud/) or [Confluent Cloud](https://www.confluent.io/confluent-cloud/). More information can be found in the [Confluent tutorial](https://docs.confluent.io/current/quickstart/ce-docker-quickstart.html) and the [HiveMQ User Guide](https://www.hivemq.com/docs/hivemq/4.3/user-guide/docker.html):
```bash
git clone https://github.com/confluentinc/cp-all-in-one
cd cp-all-in-one
git checkout 5.5.0-post
cd cp-all-in-one/
```

As we also want the HiveMQ broker in the same docker-compose network, please add the following lines at the end to `docker-compose.yaml`:
```
  hivemq:
    image: hivemq/hivemq4
    ports:
      - 8080:8080
      - 1883:1883
    hostname: hivemq
    container_name: hivemq
```

Start the environment:
```bash
# Get Confluent Platform
docker-compose up -d --build

# Verify that containers are started correctly
docker-compose ps

# Should be similar to:
#      Name                    Command               State                Ports
# --------------------------------------------------------------------------------------------------------------------------
# broker            /etc/confluent/docker/run        Up      0.0.0.0:29092->29092/tcp, 0.0.0.0:9092->9092/tcp
# connect           /etc/confluent/docker/run        Up      0.0.0.0:8083->8083/tcp, 9092/tcp
# control-center    /etc/confluent/docker/run        Up      0.0.0.0:9021->9021/tcp
# hivemq            /opt/docker-entrypoint.sh  ...   Up      0.0.0.0:1883->1883/tcp, 8000/tcp, 0.0.0.0:8080->8080/tcp
# ksqldb-cli        ksql http://localhost:8088       Up
# ksqldb-datagen    bash -c echo Waiting for K ...   Up
# ksqldb-server     /etc/confluent/docker/run        Up      0.0.0.0:8088->8088/tcp
# rest-proxy        /etc/confluent/docker/run        Up      0.0.0.0:8082->8082/tcp
# schema-registry   /etc/confluent/docker/run        Up      0.0.0.0:8081->8081/tcp
# zookeeper         /etc/confluent/docker/run        Up      0.0.0.0:2181->2181/tcp, 2888/tcp, 3888/tcp
```

#### Install Kafka MongoDB Connector and Custom Write Strategy
Install the MongoDB Kafka Connector from the [Confluent Hub](https://www.confluent.io/hub/mongodb/kafka-connect-mongodb) via these steps:
```bash
# Download the confluent-hub CLI and copy to the connect container
cd ~
curl -OL http://client.hub.confluent.io/confluent-hub-client-latest.tar.gz
docker cp confluent-hub-client-latest.tar.gz connect:/

# SSH into the connect container
docker exec -it connect /bin/bash

cd /
tar -xzf confluent-hub-client-latest.tar.gz
mv /bin/confluent-hub /usr/share/bin

# Follow the instructions on the screen and be sure to install to /usr/share/confluent-hub-components
confluent-hub install mongodb/kafka-connect-mongodb:1.1.0

# Verify that it got installed
ls -al /usr/share/confluent-hub-components/mongodb-kafka-connect-mongodb

# Done, exit from container
exit
```

Build and load the custom write strategy that uses the timeseries best-practices (we assume you cloned this repo into your home directory):

```bash
cd ~/mongodb-iot-reference/mongodb-hivemq-kafka/kafka-write-strategy

# Build the WriteStrategy
maven package
docker cp ./target/kafka-write-strategy-0.0.1-SNAPSHOT.jar connect:/usr/share/confluent-hub-components/mongodb-kafka-connect-mongodb/lib

# Restart the connect container
docker restart connect
```

Configure the **Kafka Connector for Station Information** (We simply want to replace by \_id, so we leverage "document.id.strategy" to use the provided \_id field as well as a default "writemodel.strategy" to replace by \_id):
```bash
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" --data '
  {"name": "mongo-station-information-sink",
   "config": {
     "connector.class":"com.mongodb.kafka.connect.MongoSinkConnector",
     "tasks.max":"1",
     "topics":"station",
     "connection.uri":"mongodb+srv://USERNAME:PASSWORD@YOUR_CLUSTER.mongodb.net/test?retryWrites=true&w=majority",
     "database":"citibike",
     "collection":"stations",
     "key.converter": "org.apache.kafka.connect.storage.StringConverter",
     "value.converter": "org.apache.kafka.connect.json.JsonConverter",
     "value.converter.schemas.enable": "false",
     "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInValueStrategy",
     "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy"
}}'
```

Configure the **Kafka Connector for Station Status** (here we use a custom write strategy that leverages best-practices for time series data):
```bash
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" --data '
  {"name": "mongo-station-status-sink",
   "config": {
     "connector.class":"com.mongodb.kafka.connect.MongoSinkConnector",
     "tasks.max":"1",
     "topics":"status",
     "connection.uri":"mongodb+srv://USERNAME:PASSWORD@YOUR_CLUSTER.mongodb.net/test?retryWrites=true&w=majority",
     "database":"citibike",
     "collection":"status",
     "key.converter": "org.apache.kafka.connect.storage.StringConverter",
     "value.converter": "org.apache.kafka.connect.json.JsonConverter",
     "value.converter.schemas.enable": "false",
     "writemodel.strategy": "com.mongodb.iot.demo.kafka.StationStatusWriteStrategy"
}}'
```

Test the station information via the Console Producer:
```bash
# SSH into the broker
docker exec -it broker /bin/bash

# Start the command line producer:
kafka-console-producer --broker-list localhost:9092 --topic station --property value.serializer=custom.class.serialization.JsonSerializer

# Send a message to the connector (has to be valid JSON)
# You can sent the document mulitple times with changed values and you should see it gets overwritten in the database. 
{"_id":"72","station_id":"72","has_kiosk":true,"region_id":"71","capacity":155,"name":"W 52 St & 11 Ave","short_name":"6926.01","eightd_has_key_dispenser":false,"external_id":"66db237e-0aca-11e7-82f6-3863bb44ef7c","eightd_station_services":[],"electric_bike_surcharge_waiver":false,"rental_methods":["KEY","CREDITCARD"],"rental_url":"http://app.citibikenyc.com/S6Lr/IBV092JufD?station_id=72","station_type":"classic","geometry": { "type": "Point", "coordinates": [-73.99392888,40.76727216]}}
```

Test the station status via the Console Producer:
```bash
# SSH into the broker
docker exec -it broker /bin/bash

# Start the command line producer:
kafka-console-producer --broker-list localhost:9092 --topic status --property value.serializer=custom.class.serialization.JsonSerializer

# Send a message to the connector (has to be valid JSON)
# You can send the document multiple times and should see that the bucket size increases and additonal values get added into the status Array.
{"_id":"72","station_id":"72","num_bikes_available":18,"is_installed":1,"station_status":"active","is_returning":1,"num_ebikes_available":0,"eightd_has_available_keys":false,"last_reported":1591110508,"last_updated":1591110585,"is_renting":1,"num_bikes_disabled":3,"num_docks_disabled":0,"num_docks_available":24}
```

#### Install the HiveMQ Enterprise Kafka Extension
Install the HiveMQ Kafka Enterprise extension (it will work for five hours, then you need to restart the MQTT broker):
* Download from https://www.hivemq.com/downloads/download-kafka-extension/
* Unzip into the extensions folder and provide the correct configuration file
```bash
cd ~
docker cp hivemq-kafka-extension-1.2.1.zip hivemq:/opt/hivemq-4.3.3/extensions/
docker exec -it hivemq /bin/bash
cd /opt/hivemq-4.3.3/extensions
unzip hivemq-kafka-extension-1.2.1.zip 
exit

unzip hivemq-kafka-extension-1.2.1.zip
sudo mv hivemq-kafka-extension /opt/hivemq/extensions/
```

Make a local copy of the config file:
```bash
docker cp hivemq:/opt/hivemq-4.3.3/extensions/hivemq-kafka-extension/kafka-configuration.example.xml kafka-configuration.xml
```
Make changes in `kafka-configuration.xml` and ensure the broker reachable within the docker network is used (broker:29092). For a good explanation about advertised hostnames in Confluent and Docker, pls. see [Kafka Listeners Explained](https://www.confluent.io/blog/kafka-listeners-explained/):
```xml
<kafka-configuration>
    <kafka-clusters>
        <kafka-cluster>
            <id>cluster01</id>
            <bootstrap-servers>broker:29092</bootstrap-servers>
        </kafka-cluster>

    </kafka-clusters>

    <mqtt-to-kafka-mappings>
        <mqtt-to-kafka-mapping>
            <id>mapping01</id>
            <cluster-id>cluster01</cluster-id>
            <mqtt-topic-filters>
                <mqtt-topic-filter>stations</mqtt-topic-filter>
            </mqtt-topic-filters>
            <kafka-topic>station</kafka-topic>
        </mqtt-to-kafka-mapping>

        <mqtt-to-kafka-mapping>
            <id>mapping02</id>
            <cluster-id>cluster01</cluster-id>
            <mqtt-topic-filters>
                <mqtt-topic-filter>status/#</mqtt-topic-filter>
            </mqtt-topic-filters>
            <kafka-topic>status</kafka-topic>
        </mqtt-to-kafka-mapping>
    </mqtt-to-kafka-mappings>
</kafka-configuration>
```

Bring the config back to the container, it will automatically get refreshed:
```bash
docker cp kafka-configuration.xml hivemq:/opt/hivemq-4.3.3/extensions/hivemq-kafka-extension/
```

### Gateway 
As the gateway, we are going to use the same instance on AWS as for HiveMQ and Kafka. It frequently executes python scripts to read the citibike data feed and put the data into an MQTT topic.

As a preparation, please execute the following commands on a fresh instance (assuming Amazon Linux 2, if not done already in the steps above):
```
sudo yum -y update
sudo yum -y install python3 git
sudo pip3 install pymongo dnspython requests paho-mqtt confluent-kafka
```

Create a `.cron_profile` file in the home directory. Please replace user and password with the values you created above. Also use the connection string from the step [Connect to Your Cluster](https://docs.atlas.mongodb.com/tutorial/connect-to-your-cluster/) of the Atlas Getting Started Guide. The MQTT_HOST is either the publicly available IP or hostname of your EC2 instance or the provided connection by HiveMQ Cloud.
```
export MONGO_URI="mongodb+srv://ATLAS_USERNAME:ATLAS_PASSWORD@iot-demo-XXXXX.mongodb.net/test?retryWrites=true&w=majority"
export MQTT_HOST="ec2-XXX-XXX-XXX-XXX.REGION.compute.amazonaws.com"
```

Copy the implemenation files (including all subdirectories) to your home directory for better convenience:
```
cd ~
cp -r PATH_TO_CLONED_GITHUB_REPO/monogdb-hivemq-kafka/ .
```

Add the processes to the crontab via `crontab -e` and add the following commands:
```
# We leverage crontab for a frequent execution of the import from the data feeds.
# The log files will contain the results of the latest execution for debugging purposes.

# Refresh stations every hour
0 * * * * . $HOME/.cron_profile; $HOME/station_information_publish.sh > station_information_publish.log

# Refresh status every 5 minutes
*/5 * * * * . $HOME/.cron_profile; $HOME/station_status_publish.sh > station_status_publish.log 2>&1
```

### Initialize MongoDB on the Gateway
These initialization steps could also go into the startup phases of other components as we have seen with the other reference implementations provided. Please execute this script once to initialize the proper indexes and views in MongoDB:
```
export MONGO_URI="mongodb+srv://ATLAS_USERNAME:ATLAS_PASSWORD@iot-demo-XXXXX.mongodb.net/test?retryWrites=true&w=majority"
python3 initialize_mongodb.py
```
