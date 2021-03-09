package com.example.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;

@EnableKafka
@SpringBootApplication
@Slf4j
public class KafkaConsumerApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication springApplication =
				new SpringApplicationBuilder()
						.sources(KafkaConsumerApplication.class)
						.web(WebApplicationType.NONE)
						.build();
		springApplication.run(args);
	}

	@Override
	public void run(String... args) throws Exception {
		log.info("Started consumer...");
	}

	@KafkaListener(topics = "sample-topic")
	public void listen(@Payload String message) {
		System.out.println("Received Message: " + message);
	}
}
