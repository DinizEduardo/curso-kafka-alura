package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {

        public static void main(String[] args) throws InterruptedException {
            var emailService = new br.com.alura.ecommerce.EmailService();
            try(var service = new KafkaService(EmailService.class.getSimpleName(),
                    "ECOMMERCE_SEND_EMAIL",
                    emailService::parse,
                    Email.class)){
                service.run();
            }

        }

        private void parse(ConsumerRecord<String, Email> record) {
            System.out.println("=============================================================");
            System.out.println("Sending e-mail");
            System.out.println("Key: " + record.key());
            System.out.println("Value: " + record.value());
            System.out.println("Offset: " + record.offset());
            System.out.println("Done");
    }

}
