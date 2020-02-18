package kafka.demo.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads
{
    final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);

    public static void main(String[] args)
    {
        new ConsumerDemoWithThreads().run();
    }

    public void run() {
        String boostrapServers = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "first_topic";

        // Latch to deal with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // Creating the consumer runnable
        logger.info("Creating the consumer thread.");
        ConsumerThread consumerRunnable = new ConsumerThread(latch, boostrapServers, groupId, topic);

        // Start the thread
        Thread myThread = new Thread(consumerRunnable);
        myThread.start();

        // Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            consumerRunnable.shutdown();

            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("Application got interrupted", e);
            } finally {
                logger.info("Application is closing");
            }
        }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerThread implements Runnable {

        private CountDownLatch latch;
        // Create consumer
        KafkaConsumer<String, String> consumer;

        final Logger logger = LoggerFactory.getLogger(ConsumerThread.class);

        public ConsumerThread(CountDownLatch latch, String boostrapServers, String groupId, String topic)
        {
            this.latch = latch;

            // Create consumer config
            Properties prop = new Properties();
            prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
            prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            consumer = new KafkaConsumer<String, String>(prop);

            // Subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run()
        {

            try {
                // Poll for new data in an infinity loop
                while(true)
                {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for(ConsumerRecord<String, String> record: records)
                    {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal");
            } finally {
                consumer.close();
                // Tell our main code we're done with the consumer
                latch.countDown();
            }
        }

        public void shutdown()
        {
            // This method is a especial one to interrupt consumer.poll()
            // It will throw the wakeUpException
            consumer.wakeup();
        }
    }
}
