import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author akshay.a
 */
public class TwitterProducer {
    Logger logger = LoggerFactory.getLogger((TwitterProducer.class.getName()));
    List<String> terms = Lists.newArrayList("Bitcoin");
    private String consumerKey = "QTXhijv8U0edJGs1DbExWeozB";
    private String consumerSecret = "KUI9BKWvZMrKPcHUMQFZZbQDiUDsOPsyGSQlaTAeGiAU31mTKl";
    private String token = "1135949416928632833-QP564X5MjXq26RpKeDL9UytA4q2w13";
    private String secret = "dRwfkkOeqqKnHlZLSzqsaj53WYsCOhFcNsHOOkwJDTOkL";

    public TwitterProducer() {

    }

    public static void main(String[] args) {

        new TwitterProducer().run();
    }

    public void run() {
        logger.info("Setup");

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        //create a twitter client
        Client client = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();
        //create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();
        //loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {

                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Something bad happened", e);
                        }
                    }
                });
            }
        }
        logger.info("End of application");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts twitterHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint twitterEndpoint = new StatusesFilterEndpoint();


        twitterEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication twitterAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Twitter-Client-01")                              // optional: mainly for the logs
                .hosts(twitterHosts)
                .authentication(twitterAuth)
                .endpoint(twitterEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        Client twitterClient = builder.build();
        return twitterClient;


    }

    public KafkaProducer<String, String> createKafkaProducer() {
        String bootstrapServers = "35.180.97.47:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString((Integer.MAX_VALUE)));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        //high throughput producer
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString((32 * 1024)));


        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        return producer;

    }
}
