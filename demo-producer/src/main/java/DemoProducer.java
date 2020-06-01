import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.Random;

public class DemoProducer {

    private static final String topicName = "randomNumbers";
    private static final Random random = new Random();

    private static volatile boolean isActive = true;

    private static Properties getProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return props;
    }

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerProperties());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> isActive = false));

        while (isActive) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, null, String.valueOf(random.nextDouble()));

            try {
                producer.send(record);
                System.out.println("Sent " + record);
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
