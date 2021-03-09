package com.example.kafka.producer;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@SpringBootApplication
public class KafkaProducerApplication implements CommandLineRunner {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final NewTopic newTopic;

    public KafkaProducerApplication(KafkaTemplate<String, String> kafkaTemplate,
									NewTopic newTopic) {
        this.kafkaTemplate = kafkaTemplate;
		this.newTopic = newTopic;
	}

    public static void main(String[] args) {
        SpringApplication springApplication =
                new SpringApplicationBuilder()
                        .sources(KafkaProducerApplication.class)
                        .web(WebApplicationType.NONE)
                        .build();
        springApplication.run(args);
    }

    @Override
    public void run(String... args) throws Exception {
        int i = 1;
        while (true) {
            sendMessage("This is Message-" + i);
            sleep(5000);
            i++;
        }
    }

    private void sleep(long ms){
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void sendMessage(String message) {
        ListenableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(newTopic.name(), message);

        future.addCallback(new ListenableFutureCallback<>() {

			@Override
			public void onSuccess(SendResult<String, String> result) {
				System.out.println("Sent message=[" + message +
						"] with offset=[" + result.getRecordMetadata().offset() + "]");
			}

			@Override
			public void onFailure(Throwable ex) {
				System.out.println("Unable to send message=["
						+ message + "] due to : " + ex.getMessage());
			}
		});
    }
}
