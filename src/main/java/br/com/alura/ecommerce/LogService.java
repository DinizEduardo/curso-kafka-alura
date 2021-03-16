package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogService {

    public static void main(String[] args) throws InterruptedException {
        var logService = new LogService();
        try(var service = new KafkaService(EmailService.class.getSimpleName(),
                Pattern.compile("ECOMMERCE.*"),
                logService::parse)){
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("=============================================================");
        System.out.println("Topic: " + record.topic());
        System.out.println("Key: " + record.key());
        System.out.println("Value: " + record.value());
        System.out.println("Offset: " + record.offset());
        System.out.println("Processed");
    }

}
