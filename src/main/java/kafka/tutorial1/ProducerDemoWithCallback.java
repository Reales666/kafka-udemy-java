package kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args)
    {
        // Create producer properties
        Properties prop = new Properties();
        InputStream inputStream = ProducerDemoWithCallback.class.getClassLoader().getResourceAsStream("producer.properties");

        try {
            prop.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        // Create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello world");

        // Send data
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e)
            {
                // Executed every time a record is successfully sent or an exception is thrown
                if(e != null)
                {
                    // The record was successfully sent


                } else {

                }
            }
        });

        // Fluxh data
        producer.flush();

        // Flush & close the producer
        producer.close();
    }
}
