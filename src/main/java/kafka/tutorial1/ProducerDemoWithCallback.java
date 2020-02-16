package kafka.tutorial1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args)
    {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        // Create producer properties
        Properties prop = new Properties();
        InputStream inputStream = ProducerDemoWithCallback.class.getClassLoader().getResourceAsStream("producer.properties");

        try {
            prop.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        // Create a producer record
        final ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world");

        // Send data
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e)
            {
                // Executed every time a record is successfully sent or an exception is thrown
                if(e != null)
                {
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
        });

        // Fluxh data
        producer.flush();

        // Flush & close the producer
        producer.close();
    }
}
