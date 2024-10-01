#!/bin/bash

export KAFKA_HOME=/opt/kafka_2.12-3.8.0
cd $KAFKA_HOME

nohup bin/zookeeper-server-start.sh config/zookeeper.properties &> /dev/null &
nohup bin/kafka-server-start.sh config/server.properties &> /dev/null &


bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092

bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092 <<EOF
Once upon a time
there was a developer
who spent his days and nights
in agony of debugging
EOF


# test with
# kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092