package reactive.kafka;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerFactory<K, V> {

    private final Properties properties;

    public KafkaConsumerFactory(Properties properties) {
        this.properties = properties;
    }

    public KafkaConsumer<K, V> createConsumer(String groupId, Properties propertiesOverride) {
        Properties props = getDefaultProperties();
        if (this.properties != null)
            props.putAll(this.properties);
        if (propertiesOverride != null)
            props.putAll(propertiesOverride);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        KafkaConsumer<K, V> kafkaConsumer = new KafkaConsumer<>(props);
        return kafkaConsumer;
    }

    public long getHeartbeatIntervalMs() {
        if (properties.containsKey(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG))
            return Long.parseLong(properties.getProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG));
        else
            return 3000;
    }

    private Properties getDefaultProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "2000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }
}
