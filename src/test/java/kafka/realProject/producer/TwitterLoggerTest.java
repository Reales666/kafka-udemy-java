package kafka.realProject.producer;

import static org.junit.jupiter.api.Assertions.*;

class TwitterLoggerTest {

    @org.junit.jupiter.api.Test
    void start() {
        TwitterLogger tl = new TwitterLogger();
        tl.start();
    }
}