# ikodaML

ikodaML supports machine learning on Apache Spark.

It supports the following functions:

1. Receiving data streams from remote applications
1. Monitoring incoming streams and persisting to Cassandra.
1. Making predictions in response to remote client requests.

### Streaming

-  Receives a notification from <a href= "https://github.com/amerywu/mlServer" >ikoda.mlserver </a> when a new data batch is available.
-  Opens a stream to receive the batch.
-  If the stream is sparse data (LIBSVM format), the data is in 3 parts: the LIBSVM data, the column map, and the target map (see <a href="https://github.com/amerywu/ikodaCsvLibsvmCreator/wiki/Mappings-for-LIBSVM">ikodaCsvLibsvmCreator</a> for details). Sparse data is thus maintained along with human readable target/label names and column/feature names.
-  mlServer streaming is NOT threadsafe. If it is currently processing incoming data, it will send a wait signal to clients attempting to send more data.
-  mlServer also processes streams in CSV format
