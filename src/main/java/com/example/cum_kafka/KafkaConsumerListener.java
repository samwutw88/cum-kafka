package com.example.cum_kafka;

import java.io.FileWriter;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumerListener {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerListener.class);
    private static final String LOG_FILE = "kafka-log.txt";

    @KafkaListener(topics = "demo-topic", groupId = "demo-group")
    public void listen(String message) {
        logger.info("Received message: {}", message);
        appendToFile(message);
    }

    private void appendToFile(String message) {
        try (FileWriter fw = new FileWriter(LOG_FILE, true)) {
            fw.write(message + System.lineSeparator());
        } catch (IOException e) {
            logger.error("Failed to write message to file", e);
        }
    }
}