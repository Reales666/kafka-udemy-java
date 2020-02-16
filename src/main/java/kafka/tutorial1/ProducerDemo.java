package kafka.tutorial1;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerDemo {

    public static void main(String[] args)
    {
        // Create producer properties
        Properties prop = new Properties();
        InputStream inputStream = ProducerDemo.class.getClassLoader().getResourceAsStream("producer.properties");

        try {
            prop.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        // Create a producer record
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("first_topic", "hello world");

        // Send data
        producer.send(record);

        // Fluxh data
        producer.flush();

        // Flush & close the producer
        producer.close();
    }
}
