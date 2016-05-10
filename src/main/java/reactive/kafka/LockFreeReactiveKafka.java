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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.subscriber.SignalEmitter;
import reactor.core.subscriber.SignalEmitter.Emission;


public class LockFreeReactiveKafka<K, V> extends ReactiveKafka<K, V> {

    private final KafkaProducerFactory<K, V> producerFactory;
    private final KafkaConsumerFactory<K, V> consumerFactory;

    private final Mono<KafkaProducer<K, V>> producerMono;

    private AtomicBoolean hasProducer = new AtomicBoolean();

    public LockFreeReactiveKafka() {
        this(new Properties());
    }

    public LockFreeReactiveKafka(Properties configProperties) {
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
        KafkaConsumerAdapter<K, V> consumerAdapter = new KafkaConsumerAdapter<K, V>(consumerFactory, groupId, topics, pollTimeout, null, properties);
        return consumerAdapter.consumerFlux
                              .doOnCancel(() -> consumerAdapter.cancel())
                              .map(record -> consumerAdapter.committableConsumerRecord(record));
    }

    public Flux<ConsumerRecord<K, V>> receiveAutoCommit(String groupId, Collection<String> topics,
            Duration pollTimeout, Duration commitInterval, Properties properties) {
        KafkaConsumerAdapter<K, V> consumerAdapter = new KafkaConsumerAdapter<K, V>(consumerFactory, groupId, topics, pollTimeout, commitInterval, properties);
        
        return consumerAdapter.consumerFlux
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
        private final Duration pollTimeout;
        private final EmitterProcessor<EventType> emitter;
        private final SignalEmitter<EventType> signalEmitter;
        private KafkaConsumer<K, V> consumer;
        private Flux<ConsumerRecord<K, V>> consumerFlux;
        private Flux<EventType> initFlux;
        private Flux<EventType> pollFlux;
        private Flux<EventType> commitFlux;
        private Flux<EventType> heartbeatFlux;
        private AtomicLong requestsPending = new AtomicLong();
        private AtomicBoolean needsHeartbeat = new AtomicBoolean();
        private AtomicBoolean isRunning = new AtomicBoolean();

        enum EventType {
            INIT,
            POLL,
            HEARTBEAT,
            COMMIT
        }

        KafkaConsumerAdapter(KafkaConsumerFactory<K, V> consumerFactory, 
                String groupId, Collection<String> topics,
                Duration pollTimeout, Duration commitInterval, Properties properties) {
            this.heartbeatIntervalMs = consumerFactory.getHeartbeatIntervalMs();
            this.commitIntervalMs = commitInterval == null ? Long.MAX_VALUE : commitInterval.toMillis();
            this.pollTimeout = pollTimeout;          
            emitter = EmitterProcessor.create();
            signalEmitter = emitter.connectEmitter();

            initFlux = Flux.just(EventType.INIT)
                           .doOnNext(i -> {
                                       consumer = createConsumer(consumerFactory, groupId, topics, commitInterval, properties); 
                                       emit(EventType.POLL);
                                    });
            pollFlux = Flux.from(emitter)
                           .doOnRequest(i -> {
                                        emit(EventType.POLL);
                                        requestsPending.addAndGet(i);
                                      });
            heartbeatFlux = Flux.interval(heartbeatIntervalMs)
                                .doOnSubscribe(i -> needsHeartbeat.set(true))
                                .map(i -> EventType.HEARTBEAT);
            commitFlux = Flux.interval(commitIntervalMs)
                             .map(i -> EventType.COMMIT);

            // First poll signal is from INIT. POLL should be subscribed before INIT
            // for the signal to be emitted.
            consumerFlux = Flux.merge(pollFlux, initFlux, heartbeatFlux, commitFlux)
                               .doOnCancel(() -> cancel())
                               .concatMap(eventType -> doEvent(eventType));
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            commitSync();
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        }

        public void commitSync() {
            // TODO
        }

        public Emission emit(EventType eventType) {
            Emission emission = signalEmitter.emit(eventType);
            return emission;
        }
 
        private Flux<ConsumerRecord<K, V>> doEvent(EventType eventType) {
            Flux<ConsumerRecord<K, V>> consumerRecords;
            switch (eventType) {
                case INIT:
                case POLL: consumerRecords = doPoll(); break;
                case COMMIT: doCommit(); consumerRecords = Flux.empty(); break;
                case HEARTBEAT: doHeartbeat(); consumerRecords = Flux.empty(); break;
                default: throw new IllegalArgumentException("Unexpected event type: " + eventType);
            }
            return consumerRecords;
        }

        private Flux<ConsumerRecord<K, V>> doPoll() {
            ConsumerRecords<K, V> records;
            needsHeartbeat.set(false);
            records = consumer.poll(pollTimeout.toMillis());
            if (requestsPending.decrementAndGet() > 0)
                emit(EventType.POLL);
            Flux<ConsumerRecord<K, V>> flux = records == null ? Flux.empty() : Flux.fromIterable(records);
            return flux.doOnComplete(() -> emit(EventType.POLL));
        }

        private void doCommit() {
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
                needsHeartbeat.set(false);
                consumer.commitSync(offsetMap);
            }
        }

        private void doHeartbeat() {
            if (needsHeartbeat.getAndSet(true)) {
                consumer.pause(consumer.assignment());
                consumer.poll(0);
                consumer.resume(consumer.assignment());
            }
        }

        public CommittableConsumerRecord<K, V> committableConsumerRecord(ConsumerRecord<K, V> consumerRecord) {
            return new CommittableKafkaConsumerRecord(consumerRecord);
        }

        public void onConsumed(ConsumerRecord<K, V> record, boolean commitNow) {
            consumedOffsets.put(new TopicPartition(record.topic(), record.partition()), record.offset());
            if (commitNow)
                emit(EventType.COMMIT);
        }

        public void cancel() {
            if (isRunning.getAndSet(false)) {
                doCommit();
                consumer.close();
            }
        }

        private KafkaConsumer<K, V> createConsumer(KafkaConsumerFactory<K, V> consumerFactory, String groupId, Collection<String> topics,
                Duration commitInterval, Properties properties) {

            isRunning.set(true);
            KafkaConsumer<K, V> consumer = consumerFactory.createConsumer(groupId, properties);
            consumer.subscribe(topics);
            return consumer;
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
            public void commitAsync() {
                onConsumed(consumerRecord, true);
            }
        }
    }

}
