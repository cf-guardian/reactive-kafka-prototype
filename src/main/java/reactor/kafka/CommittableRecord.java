package reactor.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface CommittableRecord<K, V> {

    ConsumerRecord<K, V> consumerRecord();

    void commitAsync();

    void commitSync();
}
