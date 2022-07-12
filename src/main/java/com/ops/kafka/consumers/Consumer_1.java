package com.ops.kafka.consumers;

import com.ops.kafka.config.KafkaConfigurations;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static com.ops.kafka.config.Constants.CONSUMER_GROUP_NAME;
import static com.ops.kafka.config.Constants.STREAMS_OUTER_JOIN_TOPIC;

public class Consumer_1 {
  public static void main(String[] args) {
    Properties updatedProps = KafkaConfigurations.getConsumerProperties();
    updatedProps.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "instance-1");
    updatedProps.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-assign-test-group");
    try (KafkaConsumer<String, String> normalConsumer =
        new KafkaConsumer<>(updatedProps)) {
      List<String> topicList = new ArrayList<>();
      topicList.add("consumer-assign-test-2");
      topicList.add("consumer-assign-test-3");
      normalConsumer.subscribe(topicList);
      while (true) {
        ConsumerRecords<String, String> consumerRecords =
            normalConsumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String, String> record : consumerRecords) {
          System.out.println(record.key() +" : "+record.value());
        }
      }
    }
  }
}
