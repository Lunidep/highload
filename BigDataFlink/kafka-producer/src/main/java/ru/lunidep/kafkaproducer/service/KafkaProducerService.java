package ru.lunidep.kafkaproducer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import ru.lunidep.kafkaproducer.config.KafkaConfig;
import ru.lunidep.kafkaproducer.models.CustomerData;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaProducerService {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final CsvReaderService csvReaderService;

    @EventListener(ApplicationReadyEvent.class)
    public void sendMessages() throws IOException {
        log.info("Starting to send messages to topic: {}", kafkaTemplate.getDefaultTopic());

        List<CustomerData> allData = csvReaderService.readAllCsvFiles();
        log.info("Read {} records from CSV files", allData.size());

        allData.stream()
                .map(data -> {
                    try {
                        return objectMapper.writeValueAsString(data);
                    } catch (Exception e) {
                        log.error("Failed to convert to JSON: {}", data, e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .forEach(jsonData -> {
                    try {
                        Message<String> message = MessageBuilder.withPayload(jsonData).build();
                        kafkaTemplate.send(message);
                        log.info("Sent JSON data to topic {}: {}", kafkaTemplate.getDefaultTopic(), jsonData);
                        sleep();
                    } catch (Exception e) {
                        log.error("Failed to send message: {}", jsonData, e);
                    }
                });

        log.info("Finished sending all messages");
    }

    private static void sleep() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread interrupted", e);
        }
    }
}
