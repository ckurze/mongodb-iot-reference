# Time Series Data in MongoDB

The Jupyter Notebooks demonstrate on a simplified basis how to handle time series data in MongoDB. For a comprehensive summary, please refer to the [Whitepaper Time Series Data and MongoDB: Best Practices Guide](https://www.mongodb.com/collateral/time-series-best-practices).

## Timeseries
A common pattern to store and retrieve time series data is to leverage the document model with the so called bucketing schema pattern. Instead of storing each measurement into a single document, multiple measurements are stored into one single document. This provides the benefits of: 
* Reducing Storage space (as less data is stored multiple times, e.g. device id and other metadata, as well as better compression ratios on larger documents)
* Reduce Index sizes (by bucket size), larger parts of the index will fit into memory and increase performance
* Reduce IO by less documents (reading time series at scale is usually IO-bound load)

## Jupyter Notebooks
We prepared three Jupyter Notebooks to make yourself familiar with the key topics to timeseries in MongoDB:

- [Time Series Basics](iot_timeseries_basic.ipynb)
- [Indexing for Time Series](iot_timeseries_indexing_details.ipynb)
- [Time Series (Pre-) Aggregation](iot_timeseries_preaggregation.ipynb)

## Useful hints for writing Aggregation Pipelines

If an aggregation pipeline can be executed on index-only attributes (i.e. a covered query), a $sort stage can force the Aggregation Pipeline to use a certain index (see SERVER-40090). The first stage(s) of aggregation pipelines can use indexes, e.g. $match and $sort. If the aggregation starts with $group, it usually does not leverage indexes, so an additional $sort or $match can be used as a hint for the aggregation pipeline - in addition to the [hint option](https://docs.mongodb.com/manual/reference/method/db.collection.aggregate/#db.collection.aggregate).

An example could be the min and max timestamp per device:
```
# Hint: All necessary attributes are in the Index, explain() reveals a performed index scan:
# IXSCAN { device: 1, min_ts: 1, max_ts: 1 }

db.tfw_generated.explain('executionStats').aggregate([
{ $sort: { device: 1 } },
{ $group: {
  _id: "$device",
  min_ts: { $min: "$min_ts" },
  max_ts: { $max: "$max_ts" }
}}
], { allowDiskUse: true } )
```
