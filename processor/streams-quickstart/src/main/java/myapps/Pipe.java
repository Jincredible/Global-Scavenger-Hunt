package myapps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
 
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Pipe {
 
    public static void main(String[] args) throws Exception {
 		Properties props = new Properties();
 		//specifies a list of host/port pairs to use for establishing the initial connection to the Kafka cluster,
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
		//gives the unique identifier of your Streams application to distinguish itself with other applications talking to the same Kafka cluster
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");    // assuming that the Kafka broker this application is talking to runs on local machine with port 9092
		//customize default serialization and deserialization libraries for the record key-value pairs in the same map
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		//in Kafka, computational logic is defined as topology.
		//use a topology builder to construct such a topology, 
		final StreamsBuilder builder = new StreamsBuilder();

		//And then create a source stream from a Kafka topic named streams-plaintext-input using this topology builder: 
		//The simplest thing we can do with this stream is to write it into another Kafka topic, say it's named streams-pipe-output
		builder.stream("streams-plaintext-input").to("streams-pipe-output");
		//builder.stream('streams-plaintext-input') is a KStream<String,String> object
    
    	//We can inspect what kind of topology is created from this builder by doing the following: 
		final Topology topology = builder.build();
		//And print its description to standard output as: 
		System.out.println(topology.describe());


		//we can now construct the Streams client with the two components we have just constructed above: 
		//the configuration map and the topology object (one can also construct a StreamsConfig object from the props map and then pass that object to the constructor, KafkaStreams have overloaded constructor functions to takes either type).  
        final KafkaStreams streams = new KafkaStreams(topology, props);
        
        //The execution won't stop until close() is called on this client. We can, for example, add a shutdown hook with a countdown latch to capture a user interrupt and close the client upon terminating this program: 
        final CountDownLatch latch = new CountDownLatch(1);
 
        // attach shutdown handler to catch control-c
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
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);

    }
}