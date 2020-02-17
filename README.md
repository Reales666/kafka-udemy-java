# Kafka Udemy course code
This code follows the code of the [Udemy Kafka course](https://gemalto.udemy.com/course/apache-kafka/).

## Kafka usefull commands: 
### Start zookeeper server: 
zookeeper-server-start.sh kafka_2.12-2.4.0/config/zookeeper.properties

### Start Kafka server: 
kafka-server-start.sh kafka_2.12-2.4.0/config/server.properties
    
### Kafka topics:
- Create a kafka topic:
kafka-topics --zookeeper 127.0.0.1:2181 --topic topic_name --create --partitions 3 --replication-factor 1
    
- List all existing topics:
kafka-topics --zookeeper 127.0.0.1:2181 --list
        
- Describe kafka topic: 
kafka-topics --zookeeper 127.0.0.1:2181 --topic topic_name --describe
        
- Delete kafka topic:
kafka-topics --zookeeper 127.0.0.1:2181 --topic topic_name --delete