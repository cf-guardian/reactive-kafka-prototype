package reactive.kafka;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import reactor.core.flow.Cancellation;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReactiveKafka<K, V> {

    private final KafkaProducerFactory<K, V> producerFactory;
    private final KafkaConsumerFactory<K, V> consumerFactory;

    private final Mono<KafkaProducer<K, V>> producerMono;

    private AtomicBoolean hasProducer = new AtomicBoolean();

    public ReactiveKafka() {
        this(new Properties());
    }

    public ReactiveKafka(Properties configProperties) {
        this.producerFactory = new KafkaProducerFactory<K, V>(configProperties);
        this.consumerFactory = new KafkaConsumerFactory<K, V>(configProperties);
        this.producerMono = Mono.fromCallable(() -> producerFactory.createProducer()).cache().doOnSubscribe(s -> hasProducer.set(true)).log();
    }

    public Mono<RecordMetadata> send(String topic, K key, V payload) {
        return send(topic, null, key, payload);
    }

    public Mono<RecordMetadata> send(String topic, Integer partition, K key, V payload) {

        return this.producerMono
                   .then(producer -> Mono.fromCompletableFuture(doSend(producer, topic, partition, key, payload)));
    }

    public Flux<CommittableConsumerRecord<K, V>> receive(String groupId, Collection<String> topics, Duration pollTimeout, Properties properties) {
        KafkaConsumerAdapter<K, V> consumerAdapter = new KafkaConsumerAdapter<K, V>(consumerFactory, groupId, topics, null, properties);
        return Flux.range(1, Integer.MAX_VALUE)
                   .doOnCancel(() -> consumerAdapter.cancel())
                   .concatMap(i -> consumerAdapter.poll(pollTimeout))
                   .map(record -> consumerAdapter.committableConsumerRecord(record));
    }

    public Flux<ConsumerRecord<K, V>> receiveAutoCommit(String groupId, Collection<String> topics,
            Duration pollTimeout, Duration commitInterval, Properties properties) {
        KafkaConsumerAdapter<K, V> consumerAdapter = new KafkaConsumerAdapter<K, V>(consumerFactory, groupId, topics, commitInterval, properties);
        return Flux.range(1, Integer.MAX_VALUE)
                   .doOnCancel(() -> consumerAdapter.cancel())
                   .concatMap(i -> consumerAdapter.poll(pollTimeout))
                   .doOnNext(record -> consumerAdapter.onConsumed(record, false));
    }

    public void close() {
        if (hasProducer.getAndSet(false))
            producerMono.get().close();
    }

    private CompletableFuture<RecordMetadata> doSend(KafkaProducer<K, V> producer, String topic, Integer partition, K key, V payload) {

        final CompletableFuture<RecordMetadata> future = new CompletableFuture<RecordMetadata>();
        producer.send(new ProducerRecord<>(topic, partition, key, payload), (metadata, exception) -> {
            if (exception == null) {
                future.complete(metadata);
            } else {
                exception.printStackTrace();
                future.completeExceptionally(exception);
            }
        });
        return future;
    }

    private static class KafkaConsumerAdapter<K, V> implements ConsumerRebalanceListener {
        private final Map<TopicPartition, Long> consumedOffsets = new ConcurrentHashMap<>();
        private final long heartbeatIntervalMs;
        private final long commitIntervalMs;
        private final Mono<KafkaConsumer<K, V>> consumerMono;
        private Cancellation heartbeatCtrl;
        private Cancellation commitCtrl;
        private ReentrantLock consumerLock = new ReentrantLock();
        private AtomicBoolean isRunning = new AtomicBoolean();

        KafkaConsumerAdapter(KafkaConsumerFactory<K, V> consumerFactory,
                String groupId, Collection<String> topics,
                Duration commitInterval, Properties properties) {
            this.heartbeatIntervalMs = consumerFactory.getHeartbeatIntervalMs();
            this.commitIntervalMs = commitInterval == null ? Long.MAX_VALUE : commitInterval.toMillis();
            consumerMono = Mono.fromCallable(() -> createConsumer(consumerFactory, groupId, topics, commitInterval, properties)).cache();
        }

        public Flux<ConsumerRecord<K, V>> poll(Duration timeout) {

            restartHeartbeats();

            ConsumerRecords<K, V> records = null;
            consumerLock.lock();
            try {
                records = getConsumer().poll(timeout.toMillis());
            } finally {
                consumerLock.unlock();
            }
            if (records == null)
                return Flux.empty();
            else
                return Flux.fromIterable(records);
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            commit(true);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        }

        public boolean commit(boolean restartPeriodicCommits) {
            boolean committed = false;
            if (restartPeriodicCommits)
                restartPeriodicCommits();
            Map<TopicPartition, OffsetAndMetadata> offsetMap = null;
            if (!consumedOffsets.isEmpty()) {     
                offsetMap = new HashMap<>();
                Iterator<Map.Entry<TopicPartition, Long>> iterator = consumedOffsets.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<TopicPartition, Long> entry = iterator.next();
                    offsetMap.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
                    iterator.remove();
                }
            }
            if (offsetMap != null) {               
                restartHeartbeats();           
                consumerLock.lock();
                try {
                    getConsumer().commitSync(offsetMap);
                    committed = true;
                } finally {
                    consumerLock.unlock();
                }
            }
            return committed;
        }

        public boolean heartbeat() {
            KafkaConsumer<K, V> consumer = getConsumer();
            consumerLock.lock();
            try {
                consumer.pause(consumer.assignment());
                consumer.poll(0);
                consumer.resume(consumer.assignment());
            } finally {
                consumerLock.unlock();
            }
            return true;
        }

        public CommittableConsumerRecord<K, V> committableConsumerRecord(ConsumerRecord<K, V> consumerRecord) {
            return new CommittableKafkaConsumerRecord(consumerRecord);
        }

        public void onConsumed(ConsumerRecord<K, V> record, boolean commitNow) {
            consumedOffsets.put(new TopicPartition(record.topic(), record.partition()), record.offset());
            if (commitNow)
                commit(false);
        }

        public void cancel() {
            if (isRunning.getAndSet(false)) {
                commit(false);
                if (heartbeatCtrl != null)
                    heartbeatCtrl.dispose();
                if (commitCtrl != null)
                    commitCtrl.dispose();
                getConsumer().close();
            }
        }

        private KafkaConsumer<K, V> createConsumer(KafkaConsumerFactory<K, V> consumerFactory,
                String groupId, Collection<String> topics,
                Duration commitInterval, Properties properties) {

            isRunning.set(true);
            KafkaConsumer<K, V> consumer = consumerFactory.createConsumer(groupId, properties);
            consumer.subscribe(topics);
            restartHeartbeats();
            if (commitInterval != null)
                restartPeriodicCommits();
            return consumer;
        }

        private KafkaConsumer<K, V> getConsumer() {
            return consumerMono.get();
        }

        private void restartHeartbeats() {
            if (heartbeatCtrl != null)
                heartbeatCtrl.dispose();
            heartbeatCtrl = Flux.interval(heartbeatIntervalMs)
                                 .map(i -> heartbeat())
                                 .subscribe();
        }

        private void restartPeriodicCommits() {
            if (commitIntervalMs < Long.MAX_VALUE) {
                if (commitCtrl != null)
                    commitCtrl.dispose();
                commitCtrl = Flux.interval(commitIntervalMs)
                                 .map(i -> commit(false))
                                 .subscribe();
            }
        }

        private class CommittableKafkaConsumerRecord implements CommittableConsumerRecord<K, V> {

            private final ConsumerRecord<K, V> consumerRecord;
            CommittableKafkaConsumerRecord(ConsumerRecord<K, V> consumerRecord) {
                this.consumerRecord = consumerRecord;
            }

            @Override
            public ConsumerRecord<K, V> getRecord() {
                return consumerRecord;
            }

            @Override
            public void commit() {
                onConsumed(consumerRecord, true);
            }
        }
    }

}
