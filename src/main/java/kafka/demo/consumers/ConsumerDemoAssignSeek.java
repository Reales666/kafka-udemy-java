package kafka.demo.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek
{
    public static void main(String[] args)
    {
        final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

        String boostrapServers = "127.0.0.1:9092";
        String groupId = "my-seventh-application";
        String topic = "first_topic";

        // Create consumer config
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        /* Possible values:
            - Earliest: you want to read from the very beginning of the topic history.
            - Latest:   Only the new messages will be displayed.
            - None: Will throw an error.
         */
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

        // Assign and seek are mostly used to replay data or fetch a specific message

        // assign
        TopicPartition partitionToReadFrom =  new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(partitionToReadFrom));

        // seek
        long offsetToReadFrom = 15L;
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        // Subscribe consumer to our topic(s)
//        consumer.subscribe(Collections.singleton(topic));
        consumer.subscribe(Arrays.asList(topic));

        int maxMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadsSoFar = 0;

        // Poll for new data in an infinity loop
        while(keepOnReading)
        {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record: records)
            {
                numberOfMessagesReadsSoFar += 1;
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());

                if(numberOfMessagesReadsSoFar >= maxMessagesToRead)
                {
                    keepOnReading = false;
                    break;
                }
            }

            logger.info("Exiting the application");
        }
    }
}
