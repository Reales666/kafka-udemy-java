package kafka.demo.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args)
    {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        // Create producer properties
        String boostrapServers = "127.0.0.1:9092";

        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        for(int i = 0; i < 10; i ++) {
            // Create a producer record

            String topic = "first_topic";
            String value = "hello world (" + i + ")!";
            String key = "id_" + i;

            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: " + key);
            // id_0 is going to partition 1
            // id_1 partition 0
            // id_2 partition 2
            // and so on...
            // You can use this info to check that the same key is always stored on the same partition.

            // Send data
            try {
                producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // Executed every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            // The record was successfully sent
                            logger.info("Received new metadata: \n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp());
                        } else {
                            logger.error("Error while producing", e);
                        }
                    }
                }).get(); // Block the .send() to make it synchronous. Don't do this in production!

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        // Fluxh data
        producer.flush();

        // Flush & close the producer
        producer.close();
    }
}
