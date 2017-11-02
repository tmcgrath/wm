## POC ML with Cassandra and Kafka


#### Train Model
Dependency is training data in Cassandra.  See README.txt in data/

Update `com.supergloo.ml.streaming.UberTrain` for Cassandra and file location
paths.  This is just POC, so lots of hardcoding

Compile and run com.supergloo.ml.streaming.UberTrain


### To use Model
Dependency is running Kafka with a topic named "raw_uber".

Update `com.supergloo.ml.streaming.UberStream` for Kafka and file location
of saved model from TrainModel step

Compile and run com.supergloo.ml.streaming.UberStream

You can send test data to the Kafka stream with kafka cli tools; i.e.

```kafka-console-producer.sh --broker-list localhost:9092 --topic raw_uber
  --new-producer < sample-uber.csv```