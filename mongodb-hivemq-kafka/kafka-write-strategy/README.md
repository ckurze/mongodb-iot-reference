# How to build a custom WriteStrategy
A simple MongoDB Kafka connector custom write strategy to get you started.

For more information, please see the some more examples by @felixreichenbach: https://github.com/felixreichenbach/KafkaSinkConnectorCustomizations

## Build
```
mvn package
```

## Deploy Custom Write Strategy JAR File
Copy the jar file into the "connect" container:
```
docker cp ./target/kafka-write-strategy-1.0-SNAPSHOT.jar connect:/usr/share/confluent-hub-components/kafka-connect-mongodb/lib/kafka-write-strategy-1.0-SNAPSHOT.jar
```

## Restart the container “connect”:
```
docker restart connect
```

## Configure the MongoDB-Kafka-Connector

Create MongoDB Sink Connector using the Custom Write Strategy:
```
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
## Test Custom Write Strategy
We are going to use the command line producer to create messages.
The “broker” container can be used to run Kafka cli commands.

```bash
# SSH into the broker
docker exec -it broker /bin/bash

# Start the command line producer:
kafka-console-producer --broker-list localhost:9092 --topic status --property value.serializer=custom.class.serialization.JsonSerializer

# Send a message to the connector (has to be valid JSON)
# You can send the document multiple times and should see that the bucket size increases and additonal values get added into the status Array.
{"_id":"72","station_id":"72","num_bikes_available":18,"is_installed":1,"station_status":"active","is_returning":1,"num_ebikes_available":0,"eightd_has_available_keys":false,"last_reported":1591110508,"last_updated":1591110585,"is_renting":1,"num_bikes_disabled":3,"num_docks_disabled":0,"num_docks_available":24}
```
