package com.ops.kafka.streams;

import com.ops.kafka.config.KafkaConfigurations;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import static com.ops.kafka.config.Constants.*;

public class KafkaStreamsApp {
  public static void main(String[] args) {
    // Get the source stream.
    final StreamsBuilder builder = new StreamsBuilder();

    // Implement streams logic.
    final KStream<String, String> source = builder.stream(STREAMS_INPUT_TOPIC);
    source
        .flatMap(
            (key, value) -> {
              List<KeyValue<String, String>> result = new LinkedList<>();
              System.out.println("----------- Original record -----------------");
              System.out.println("Input Key : " + key);
              System.out.println("Input Value : " + value);
              String modifiedValue = value + "something";
              result.add(new KeyValue<>(key, modifiedValue));
              System.out.println("----------- Modified record -----------------");
              System.out.println("Output Key : " + key);
              System.out.println("Output Value : " + modifiedValue);

              return result;
            })
        .to(STREAMS_OUTPUT_TOPIC);

    final Topology topology = builder.build();
    final KafkaStreams streams =
        new KafkaStreams(topology, KafkaConfigurations.getStreamsProperties());
    // Print the topology to the console.
    System.out.println(topology.describe());
    final CountDownLatch latch = new CountDownLatch(1);

    // Attach a shutdown handler to catch control-c and terminate the application gracefully.
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread("streams-shutdown-hook") {
              @Override
              public void run() {
                streams.close();
                latch.countDown();
              }
            });

    try {
      streams.start();
      latch.await();
    } catch (final Throwable e) {
      System.out.println(e.getMessage());
      System.exit(1);
    }
    System.exit(0);
  }
}
