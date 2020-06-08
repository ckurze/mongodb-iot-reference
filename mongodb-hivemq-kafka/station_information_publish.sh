#!/bin/sh

if [[ -z "$MQTT_HOST" ]]
then
      echo "\$MQTT_HOST is empty - process will not run."
else
      python3 station_information_publish.py
fi
