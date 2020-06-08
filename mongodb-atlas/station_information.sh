#!/bin/sh

if [ -z "$MONGO_URI" ]
then
      echo "\$MONGO_URI is empty - process will not run."
else
      python3 station_information.py
fi
