package com.ops.kafka.streams;

import com.ops.kafka.config.KafkaConfigurations;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import static com.ops.kafka.config.Constants.*;

public class KStreamsJoins {
  public static void main(String[] args) {
    // Get the source stream.
    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, String> orderLeft = builder.stream(STREAMS_ORDER_LEFT_TOPIC);
    final KStream<String, String> profileRight = builder.stream(STREAMS_PROFILE_RIGHT_TOPIC);

    /* Inner Join */
    orderLeft.join(profileRight,(leftOrderValue, rightProfileValue) -> leftOrderValue+" is bought by "+rightProfileValue, JoinWindows.of(Duration.ofMinutes(5)))
            .to(STREAMS_INNER_JOIN_TOPIC);

    /* Left Join */
    orderLeft.leftJoin(profileRight,(leftOrderValue, rightProfileValue) -> "left= "+leftOrderValue+"right="+rightProfileValue, JoinWindows.of(Duration.ofMinutes(5)))
            .to(STREAMS_LEFT_JOIN_TOPIC);

    /* Outer Join */
    orderLeft.outerJoin(profileRight,(leftOrderValue, rightProfileValue) -> "left= "+leftOrderValue+"right="+rightProfileValue, JoinWindows.of(Duration.ofMinutes(5)))
            .to(STREAMS_OUTER_JOIN_TOPIC);



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
