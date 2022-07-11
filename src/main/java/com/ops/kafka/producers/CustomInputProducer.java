package com.ops.kafka.producers;

import com.ops.kafka.config.KafkaConfigurations;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Scanner;

import static com.ops.kafka.config.Constants.STREAMS_INPUT_TOPIC;

public class CustomInputProducer {
  public static void main(String[] args) {
    try (KafkaProducer<String, String> customProducer =
        new KafkaProducer<>(KafkaConfigurations.getProducerProperties())) {

      Scanner scanner = new Scanner(System.in);
      System.out.println("Enter your message");
      while (scanner.hasNext()) {
        String msg = scanner.next();
        String[] split = msg.split(":");
        customProducer.send(new ProducerRecord<>(STREAMS_INPUT_TOPIC, split[0], split[1]));
      }
    }
  }
}
