# ikodaML

### Overview

ikodaML supports machine learning on Apache Spark.

It supports the following functions:

1. Receiving data streams from remote applications
1. Monitoring incoming streams and persisting to Cassandra.
1. Making predictions in response to remote client requests.
1. Running highly flexible and recursive data analysis pipelines through simple configurations. 

### Streaming

-  Receives a notification from <a href= "https://github.com/amerywu/mlServer" >ikoda.mlserver </a> when a new data batch is available.
-  Opens a stream to receive the batch.
-  If the stream is sparse data (LIBSVM format), the data is in 3 parts: the LIBSVM data, the column map, and the target map (see <a href="https://github.com/amerywu/ikodaCsvLibsvmCreator/wiki/Mappings-for-LIBSVM">ikodaCsvLibsvmCreator</a> for details). Sparse data is thus maintained along with human readable target/label names and column/feature names.
-  mlServer streaming is NOT threadsafe. If it is currently processing incoming data, it will send a wait signal to clients attempting to send more data.
-  mlServer also processes streams in CSV format

### Persisting to Cassandra
-  ikodaML is highly configurable. It processes different data sources with a range of different processing rules.
-  ikodaML can also generate a generic keyspace. This keyspace supports sparse NLP data from a specific unknown source. It maintains  mappings to human friendly target/label names and column/feature names and also supports the creation of supplementary tables fro non-sparse data.
-  ikodaML monitors incoming data and persists the data to Cassandra.
-  ikodaML may either persist to a final keyspace in Cassandra or to a staging keyspace. At a prespecified interval, ikodaML can check the data for integrity and follow data cleaning routines. At a prespecified threshold, ikodaMajors moves the data to the final keyspace.

### Predicting
- 
