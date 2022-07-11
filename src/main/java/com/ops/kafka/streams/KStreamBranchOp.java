package com.ops.kafka.streams;

import com.ops.kafka.config.KafkaConfigurations;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import static com.ops.kafka.config.Constants.*;

public class KStreamBranchOp {
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

      evenValueStream
        .peek(
            (key, value) -> {
              System.out.println("Key : " + key);
              System.out.println("Value : " + value);
            })
        .merge(oddValueStream)
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
