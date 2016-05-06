package reactive.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerFactory<K, V> {

    private final Properties properties;

    public KafkaProducerFactory(Properties properties) {
        this.properties = properties;
    }

    public KafkaProducer<K, V> createProducer() {
        Properties props = getDefaultProperties();
        props.putAll(this.properties);
        KafkaProducer<K, V> kafkaProducer = new KafkaProducer<>(props);
        return kafkaProducer;
    }

    private Properties getDefaultProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }
}
