package reactive.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface CommittableConsumerRecord<K, V> {

    public ConsumerRecord<K, V> getRecord();

    public void commit();
}
