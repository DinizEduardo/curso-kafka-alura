package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;

public class EmailService {

        public static void main(String[] args) throws InterruptedException {
            var emailService = new br.com.alura.ecommerce.EmailService();
            try(var service = new KafkaService(EmailService.class.getSimpleName(),
                    "ECOMMERCE_SEND_EMAIL",
                    emailService::parse,
                    String.class,
                    Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))){
                service.run();
            }

        }

        private void parse(ConsumerRecord<String, String> record) {
            System.out.println("=============================================================");
            System.out.println("Sending e-mail");
            System.out.println("Key: " + record.key());
            System.out.println("Value: " + record.value());
            System.out.println("Offset: " + record.offset());
            System.out.println("Done");
    }

}
