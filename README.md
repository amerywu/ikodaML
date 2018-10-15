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
- ikodaML can generate data models utilizing any of the model generating tools available in Apache Spark.
- ikodaML functions as a server for client queries. These queries typically involve the input of natural language data. ikodaML parses the data, matches it to a dataschema for the model and generates a prediction. The prediction is the server response to the client.

### Analysis Pipeline
- ikodaML retrieves large data sets from Cassandra for data analysis and model generation.
- A simple configuration utility (along with the elegance of functional programming) allows for data to be analyzed through any number of phases and in any order. 
- The pipeline allows for recursion, with different analysis instructions provided to the overall dataset and the subsets.
- Developing new steps in the pipeline is highly encapsulated and almost trivial.

#### ikodaML is *not* intended for open sharing. Hence, documentation is limited.
Nonetheless,a brief <a href="https://amerywu.github.io/ikodaML/scaladoc/index.html#package">API</a> is available

