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

You can send test data to the Kafka stream on your laptop with kafka cli tools; i.e.


* Start Zookeeper ```bin/zookeeper-server-start.sh config/zookeeper.properties```
* Start Kafka ```bin/kafka-server-start.sh config/server.properties```
* Create Kafka topic ```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic raw_uber```
* Send Uber Data to Kafka ```bin/kafka-console-producer.sh --broker-list localhost:9092 --topic raw_uber
  --new-producer < sample-uber.csv```