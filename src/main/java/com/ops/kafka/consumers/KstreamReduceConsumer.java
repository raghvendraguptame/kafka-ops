package com.ops.kafka.consumers;

import com.ops.kafka.config.KafkaConfigurations;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static com.ops.kafka.config.Constants.STREAMS_REDUCE_OUTPUT_TOPIC;

public class KstreamReduceConsumer {

  public static void main(String[] args) {
    Properties updatedProps = KafkaConfigurations.getConsumerProperties();
//    updatedProps.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "instance-1");
    try (KafkaConsumer<String, String> normalConsumer =
        new KafkaConsumer<>(updatedProps)) {
      normalConsumer.subscribe(Collections.singleton(STREAMS_REDUCE_OUTPUT_TOPIC));
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
