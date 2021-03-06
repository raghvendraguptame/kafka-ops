package com.ops.kafka.streams;

import com.ops.kafka.config.KafkaConfigurations;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.ops.kafka.config.Constants.*;

public class KStreamsStatefulOps {
  public static void main(String[] args) {
    // Get the source stream.
    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, String> source = builder.stream(STREAMS_INPUT_TOPIC);

    KGroupedStream<String, String> groupedStream = source.groupByKey();
    /* Aggregate the length of value for a specific key*/
    groupedStream
        .aggregate(
            () -> 0,
            (aggKey, newValue, aggValue) -> aggValue + newValue.length(),
            Materialized.with(Serdes.String(), Serdes.Integer()))
        .toStream()
        .peek(
            (key, value) -> {
              System.out.println("Key : " + key);
              System.out.println("Value : " + value);
            })
        .to(STREAMS_AGG_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Integer()));

    /* Count the number of msg in the topic for a specific key*/
    groupedStream
        .count(Materialized.with(Serdes.String(), Serdes.Long()))
        .toStream()
        .peek(
            (key, value) -> {
              System.out.println("Key : " + key);
              System.out.println("Value : " + value);
            })
        .to(STREAMS_COUNT_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

    /* Reduce, combines the msg for a specific key*/
    groupedStream
        .reduce((aggValue, newValue) -> aggValue + " + " +newValue)
        .toStream()
        .peek(
            (key, value) -> {
              System.out.println("Key : " + key);
              System.out.println("Value : " + value);
            })
        .to(STREAMS_REDUCE_OUTPUT_TOPIC);

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
