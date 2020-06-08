#!/bin/sh

if [[ -z "$MQTT_HOST" || -z "$MONGO_URI" ]]
then
      echo "\$MQTT_HOST or \$MONGO_URI is empty - process will not run."
else
      python3 station_status_publish.py
fi
