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
        private final AtomicBoolean commitPending = new AtomicBoolean();
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
            stopHeartbeats();

            ConsumerRecords<K, V> records = null;
            consumerLock.lock();
            try {
                if (commitPending.getAndSet(false))
                    commit(true, true);
                records = getConsumer().poll(timeout.toMillis());
            } finally {
                consumerLock.unlock();
                startHeartbeats();
            }
            if (records == null)
                return Flux.empty();
            else
                return Flux.fromIterable(records);
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            commit(true, true);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        }

        public boolean commit(boolean restartPeriodicCommits, boolean force) {
            boolean committed = false;
            if (!consumedOffsets.isEmpty()) {
                if (restartPeriodicCommits)
                    stopPeriodicCommits();
                boolean locked;
                if (force) {
                    locked = true;
                    consumerLock.lock();
                } else
                    locked = consumerLock.tryLock();
                if (locked) {
                    stopHeartbeats();
                    try {
                        committed = commit();
                    } finally {
                        consumerLock.unlock();
                        startHeartbeats();
                    }
                } else {
                    commitPending.set(true);
                }
                if (restartPeriodicCommits)
                    startPeriodicCommits();
            }
            return committed;
        }

        private boolean commit() {
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
            if (offsetMap != null)
                getConsumer().commitSync(offsetMap);
            return offsetMap != null;
        }

        public boolean heartbeat() {
            boolean done = false;
            KafkaConsumer<K, V> consumer = getConsumer();
            if (consumerLock.tryLock()) {
                try {
                    consumer.pause(consumer.assignment());
                    consumer.poll(0);
                    consumer.resume(consumer.assignment());
                    done = true;
                } finally {
                    consumerLock.unlock();
                }
            }
            return done;
        }

        public CommittableConsumerRecord<K, V> committableConsumerRecord(ConsumerRecord<K, V> consumerRecord) {
            return new CommittableKafkaConsumerRecord(consumerRecord);
        }

        public void onConsumed(ConsumerRecord<K, V> record, boolean commitNow) {
            consumedOffsets.put(new TopicPartition(record.topic(), record.partition()), record.offset());
            if (commitNow)
                commit(false, false);
        }

        public void cancel() {
            if (isRunning.getAndSet(false)) {
                commit(false, true);
                if (heartbeatCtrl != null)
                    heartbeatCtrl.dispose();
                if (commitCtrl != null)
                    commitCtrl.dispose();
                getConsumer().close();
            }
        }

        private KafkaConsumer<K, V> createConsumer(KafkaConsumerFactory<K, V> consumerFactory, String groupId, Collection<String> topics,
                Duration commitInterval, Properties properties) {

            isRunning.set(true);
            KafkaConsumer<K, V> consumer = consumerFactory.createConsumer(groupId, properties);
            consumer.subscribe(topics);
            startHeartbeats();
            if (commitInterval != null)
                startPeriodicCommits();
            return consumer;
        }

        private KafkaConsumer<K, V> getConsumer() {
            return consumerMono.get();
        }

        private void stopHeartbeats() {
            if (heartbeatCtrl != null)
                heartbeatCtrl.dispose();
        }

        private void startHeartbeats() {
            heartbeatCtrl = Flux.interval(heartbeatIntervalMs).map(i -> heartbeat()).subscribe();
        }

        private void stopPeriodicCommits() {
            if (commitIntervalMs < Long.MAX_VALUE) {
                if (commitCtrl != null)
                    commitCtrl.dispose();
            }
        }

        private void startPeriodicCommits() {
            if (commitIntervalMs < Long.MAX_VALUE) {
                commitCtrl = Flux.interval(commitIntervalMs).map(i -> commit(false, false)).subscribe();
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
