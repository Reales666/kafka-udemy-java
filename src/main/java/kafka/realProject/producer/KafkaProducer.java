package kafka.realProject.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducer
{
    private Properties conf;
    private String topic;

    public KafkaProducer(String boostrapServers, String topic)
    {
        this.topic = topic;

        conf = new Properties();
        conf.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        conf.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        conf.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    public void SendMessage(String message)
    {
        // Create the producer
        org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(conf);

        // Create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, message);

        // Send data
        producer.send(record);

        // Fluxh data
        producer.flush();

        // Flush & close the producer
        producer.close();
    }
}
