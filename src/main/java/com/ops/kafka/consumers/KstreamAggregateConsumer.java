package com.ops.kafka.consumers;

import com.ops.kafka.config.KafkaConfigurations;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static com.ops.kafka.config.Constants.*;

public class KstreamAggregateConsumer {


  public static void main(String[] args) {
    Properties updatedProps = KafkaConfigurations.getConsumerProperties();
    updatedProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
    updatedProps.put(ConsumerConfig.GROUP_ID_CONFIG, "integer-kstreams-consumer");
    try (KafkaConsumer<String, Integer> normalConsumer =
        new KafkaConsumer<>(updatedProps)) {
      normalConsumer.subscribe(Collections.singleton(STREAMS_AGG_OUTPUT_TOPIC));
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
