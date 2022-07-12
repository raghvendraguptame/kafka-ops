package com.ops.kafka.consumers;

import com.ops.kafka.config.KafkaConfigurations;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static com.ops.kafka.config.Constants.STREAMS_COUNT_OUTPUT_TOPIC;

public class KstreamCountConsumer {


  public static void main(String[] args) {
    Properties updatedProps = KafkaConfigurations.getConsumerProperties();
    updatedProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
    updatedProps.put(ConsumerConfig.GROUP_ID_CONFIG, "long-kstreams-consumer");
    updatedProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    try (KafkaConsumer<String, Integer> normalConsumer =
        new KafkaConsumer<>(updatedProps)) {
      normalConsumer.subscribe(Collections.singleton(STREAMS_COUNT_OUTPUT_TOPIC));
      while (true) {
        ConsumerRecords<String, Integer> consumerRecords =
            normalConsumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String, Integer> record : consumerRecords) {
          System.out.println(record.key() +" : "+record.value());
        }
      }
    }
  }
}
