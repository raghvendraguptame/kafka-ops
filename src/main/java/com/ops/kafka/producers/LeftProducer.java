package com.ops.kafka.producers;

import com.ops.kafka.config.KafkaConfigurations;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Scanner;

import static com.ops.kafka.config.Constants.STREAMS_INPUT_TOPIC;
import static com.ops.kafka.config.Constants.STREAMS_ORDER_LEFT_TOPIC;

public class LeftProducer {
  public static void main(String[] args) {
    try (KafkaProducer<String, String> customProducer =
        new KafkaProducer<>(KafkaConfigurations.getProducerProperties())) {

      Scanner scanner = new Scanner(System.in);
      System.out.println("Enter your message");
      System.out.println("consumer-assign-test-3");
      while (scanner.hasNext()) {
        String msg = scanner.next();
        String[] split = msg.split(":");
        customProducer.send(new ProducerRecord<>("consumer-assign-test-3", split[0], split[1]));
      }
    }
  }
}
