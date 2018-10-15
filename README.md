# ikodaML

ikodaML supports machine learning on Apache Spark.

It supports the following functions:

1. Receiving data streams from remote applications
1. Monitoring incoming streams and persisting to Cassandra.
1. Making predictions in response to remote client requests.

### Streaming

Receives a notification from ikoda.mlserver when a new data batch is available.
Opens a stream to receive the batch
