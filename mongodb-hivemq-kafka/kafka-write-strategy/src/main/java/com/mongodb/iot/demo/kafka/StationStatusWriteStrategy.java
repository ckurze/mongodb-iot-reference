package com.mongodb.iot.demo.kafka;

import java.util.Calendar;
import java.util.Date;

import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.writemodel.strategy.WriteModelStrategy;

public class StationStatusWriteStrategy implements WriteModelStrategy {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(StationStatusWriteStrategy.class);
    private static final UpdateOptions UPDATE_OPTIONS = new UpdateOptions().upsert(true);

    // Incoming JSON should be a proper citibike station status like { "station_id":"1234", "last_udpated": "...", "last_reported": "...", ... } 
    @Override
    public WriteModel<BsonDocument> createWriteModel(final SinkDocument document) {
        
        // Retrieve the value part of the SinkDocument
        BsonDocument valueDocument = document.getValueDoc().orElseThrow(
                () -> new DataException("Error: Cannot build the WriteModel since the value document was missing unexpectedly."));

        // We expect correct messages here, should be more robust in terms of error handling
        if (!valueDocument.containsKey("station_id")) {
        	throw new DataException("Error: Cannot build the WriteModel since the station does not have a station_id attribute.");
        }
        
        // Store stationId and remove from document
        BsonString stationId = valueDocument.getString("station_id");
        valueDocument.remove("station_id");
        valueDocument.remove("_id");
         
        // Format some data from string to date for better readability
        Date lastUpdated = new Date(valueDocument.get("last_updated").asNumber().longValue()*1000);
        valueDocument.put("ts", new BsonDateTime(lastUpdated.getTime()));
        valueDocument.put("last_reported", new BsonDateTime(valueDocument.get("last_reported").asNumber().longValue()*1000));
        valueDocument.remove("last_updated");
        
        // Define the filter part of the update statement, i.e.
        // the station we want to update
        // the maximum bucket size
        BsonDocument filters = new BsonDocument();
        filters.append("station_id", stationId);
        filters.append("bucket_size", new BsonDocument("$lt", new BsonInt32(120)));
        
        // Define the modifications we want to make to the data
        BsonDocument updates = new BsonDocument();
        // Push the well-formatted value document - needs more error handling and checking in production
        updates.append("$push", new BsonDocument("status", valueDocument));
        // Increment the bucket size by one
        updates.append("$inc", new BsonDocument("bucket_size", new BsonInt32(1)));
        // Set the max value for the max timestamp of the document
        // Set the max value for the TTL index
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(lastUpdated);
        calendar.add(Calendar.HOUR_OF_DAY, 12);
        Date ttlDate = calendar.getTime();
        BsonDocument maxDoc = new BsonDocument();
        maxDoc.append("max_ts", new BsonDateTime(lastUpdated.getTime()));
        maxDoc.append("expire_on", new BsonDateTime(ttlDate.getTime()));
        updates.append("$max", maxDoc);
        
        // Set the min value for the min timestamp of the document
        updates.append("$min", new BsonDocument("min_ts", new BsonDateTime(lastUpdated.getTime())));
        
        // Return the full update
        return new UpdateOneModel<BsonDocument>(
                filters,
                updates,
                UPDATE_OPTIONS
        );
    }
}
