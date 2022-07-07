package com.ops.kafka.streams;

import com.ops.kafka.config.KafkaConfigurations;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.concurrent.CountDownLatch;
import static com.ops.kafka.config.Constants.*;

public class KafkaStreamsApp {
  public static void main(String[] args) {
      // Get the source stream.
      final StreamsBuilder builder = new StreamsBuilder();

      //Implement streams logic.
      final KStream<String,String> source = builder.stream(STREAMS_INPUT_TOPIC);
      KStream<String,String>[] branches = source.branch(((key, value) -> Integer.getInteger(key) % 2==0),((key, value) -> true));
      KStream<String,String> evenKeysStream = branches[0];
      KStream<String,String> oddKeyStream = branches[1];

      final Topology topology = builder.build();
      final KafkaStreams streams = new KafkaStreams(topology, KafkaConfigurations.getStreamsProperties());
      // Print the topology to the console.
      System.out.println(topology.describe());
      final CountDownLatch latch = new CountDownLatch(1);

      // Attach a shutdown handler to catch control-c and terminate the application gracefully.
      Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
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
