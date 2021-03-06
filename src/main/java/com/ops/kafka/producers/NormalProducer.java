package com.ops.kafka.producers;

import com.ops.kafka.config.KafkaConfigurations;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import static com.ops.kafka.config.Constants.*;

public class NormalProducer {
  public static void main(String[] args) {
    try (KafkaProducer<String, String> normalProducer =
        new KafkaProducer<>(KafkaConfigurations.getProducerProperties())) {
      for(int i=1; i<=99;i=i+2){
        normalProducer.send(new ProducerRecord<>(STREAMS_INPUT_TOPIC,"key1",Integer.toString(i)));
        normalProducer.send(new ProducerRecord<>(STREAMS_INPUT_TOPIC,"key2",Integer.toString(i+1)));
//        normalProducer.send(new ProducerRecord<>(STREAMS_INPUT_TOPIC,"key3",Integer.toString(i+2)));
//        normalProducer.send(new ProducerRecord<>(STREAMS_INPUT_TOPIC,"key4",Integer.toString(i+3)));
      }

    }
  }
}
