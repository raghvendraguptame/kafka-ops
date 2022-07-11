package com.ops.kafka.streams;

import com.ops.kafka.config.KafkaConfigurations;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import static com.ops.kafka.config.Constants.*;

public class KStreamsStatelessOps {
  public static void main(String[] args) {
    // Get the source stream.
    final StreamsBuilder builder = new StreamsBuilder();

    // Implement streams logic.
    final KStream<String, String> source = builder.stream(STREAMS_INPUT_TOPIC);
    //        KStream<String,String>[] branches = source.branch(((key, value) ->
    // Integer.getInteger(key) % 2==0));
    //        KStream<String,String> evenKeysStream = branches[0];

    Map<String, KStream<String, String>> branches =
        source
            .split(Named.as("branch-"))
            .branch((key, value) -> key.equalsIgnoreCase("key1"))
            .branch((key, value) -> key.equalsIgnoreCase("key2"))
            .noDefaultBranch();

    KStream<String, String> oddValueStream = branches.get("branch-1");
    KStream<String, String> evenValueStream = branches.get("branch-2");

      source
        .peek(
            (key, value) -> {
              System.out.println("Key : " + key);
              System.out.println("Value : " + value);
            })
              .filter((key, value) -> Integer.parseInt(value) % 2 == 1)
              .map((key, value) -> KeyValue.pair(key,value+"map"))
              .flatMap((key, value) ->{
                  List<KeyValue<String,String>> records = new LinkedList<>();
                  records.add(KeyValue.pair(key,value+" flatmap1"));
                  records.add(KeyValue.pair(key,value+" flatmap2"));
                  return records;
              })
              //not defining foreach as it will terminate the operation and we need to push the transformed
              // data to destination topic
        .to(STREAMS_OUTPUT_TOPIC);

    //        evenKeysStream.merge(oddKeysStream).to(STREAMS_OUTPUT_TOPIC);

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
