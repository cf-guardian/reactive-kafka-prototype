package reactor.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.flow.Cancellation;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.subscriber.SubmissionEmitter;

/*
 * TODO: Unsubscribe: Do we need this?
 *
 */
public class KafkaFlux<K, V> extends Flux<CommittableRecord<K, V>> implements ConsumerRebalanceListener, OffsetCommitCallback {

    private static final Logger log = LoggerFactory.getLogger(KafkaFlux.class.getName());

    private final Map<TopicPartition, Long> consumedOffsets = new ConcurrentHashMap<>();
    private final Duration pollTimeout;
    private final EmitterProcessor<EventType> emitter;
    private final SubmissionEmitter<EventType> submissionEmitter;
    private KafkaConsumer<K, V> consumer;
    private Flux<CommittableRecord<K, V>> consumerFlux;
    private Flux<EventType> commitFlux;
    private List<Flux<EventType>> fluxList = new ArrayList<>();
    private List<Flux<EventType>> periodicFluxList = new ArrayList<>();
    private List<Cancellation> cancellations = new ArrayList<>();
    private List<Consumer<Collection<SeekablePartition>>> assignListeners = new ArrayList<>();
    private List<Consumer<Collection<SeekablePartition>>> revokeListeners = new ArrayList<>();
    private Consumer<Map<TopicPartition, OffsetAndMetadata>> commitListener;
    private Consumer<Exception> commitErrorListener;
    private AtomicLong requestsPending = new AtomicLong();
    private AtomicBoolean needsHeartbeat = new AtomicBoolean();
    private Semaphore consumerSemaphore = new Semaphore(1);
    private AtomicBoolean isRunning = new AtomicBoolean();

    enum EventType {
        INIT, POLL, HEARTBEAT, COMMIT
    }

    public static <K, V> KafkaFlux<K, V> listenOn(KafkaContext<K, V> context, String groupId, Collection<String> topics) {
        Consumer<KafkaFlux<K, V>> subscriber = (flux) -> flux.consumer.subscribe(topics, flux);
        return new KafkaFlux<>(context, subscriber, groupId);
    }

    public static <K, V> KafkaFlux<K, V> listenOn(KafkaContext<K, V> context, String groupId, Pattern pattern) {
        Consumer<KafkaFlux<K, V>> subscriber = (flux) -> flux.consumer.subscribe(pattern, flux);
        return new KafkaFlux<>(context, subscriber, groupId);
    }

    public static <K, V> KafkaFlux<K, V> assign(KafkaContext<K, V> context, String groupId, Collection<TopicPartition> topicPartitions) {
        Consumer<KafkaFlux<K, V>> subscriber = (flux) -> {
            flux.consumer.assign(topicPartitions);
            flux.onPartitionsAssigned(topicPartitions);
        };
        return new KafkaFlux<>(context, subscriber, groupId);
    }

    public KafkaFlux(KafkaContext<K, V> context, Consumer<KafkaFlux<K, V>> kafkaSubscriber, String groupId) {
        log.debug("Created Kafka flux", groupId);
        this.pollTimeout = context.getPollTimeout();
        emitter = EmitterProcessor.create();
        submissionEmitter = emitter.connectEmitter();

        Flux<EventType> initFlux =
                Flux.just(EventType.INIT)
                    .doOnNext(i -> {
                            try {
                                isRunning.set(true);
                                consumer = context.getConsumerFactory().createConsumer(groupId);
                                kafkaSubscriber.accept(this);
                                emit(EventType.POLL);
                            } catch (Throwable t) {
                                log.error("Unexpected exception", t);
                                throw new RuntimeException(t);
                            }
                        });
        Flux<EventType> pollFlux =
                Flux.from(emitter)
                    .doOnRequest(i -> {
                            if (requestsPending.addAndGet(i) > 0)
                                emit(EventType.POLL);
                        });
        Flux<EventType> heartbeatFlux =
                Flux.interval(context.getConsumerFactory().getHeartbeatIntervalMs())
                     .doOnSubscribe(i -> needsHeartbeat.set(true))
                     .doOnNext(i -> doEvent(EventType.HEARTBEAT))
                     .map(i -> EventType.HEARTBEAT);

        // First poll signal is from INIT. POLL should be subscribed before INIT
        // for the signal to be emitted.
        fluxList.add(pollFlux);
        fluxList.add(initFlux);
        periodicFluxList.add(heartbeatFlux);
    }

    public KafkaFlux<K, V> autoCommit(Duration commitInterval) {
        commitFlux = Flux.interval(commitInterval)
                         .doOnNext(i -> doEvent(EventType.COMMIT))
                         .map(i -> EventType.COMMIT);
        periodicFluxList.add(commitFlux);
        return this;
    }

    public KafkaFlux<K, V> doOnCommit(Consumer<Map<TopicPartition, OffsetAndMetadata>> onCommit) {
        if (commitListener != null)
            throw new IllegalStateException("Commit listener already set");
        this.commitListener = onCommit;
        return this;
    }

    public KafkaFlux<K, V> doOnCommitError(Consumer<Exception> onCommitError) {
        if (commitErrorListener != null)
            throw new IllegalStateException("Commit error listener already set");
        this.commitErrorListener = onCommitError;
        return this;
    }

    public KafkaFlux<K, V> doOnPartitionsAssigned(Consumer<Collection<SeekablePartition>> onAssign) {
        if (onAssign != null)
            assignListeners.add(onAssign);
        return this;
    }

    public KafkaFlux<K, V> doOnPartitionsRevoked(Consumer<Collection<SeekablePartition>> onRevoke) {
        if (onRevoke != null)
            revokeListeners.add(onRevoke);
        return this;
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.debug("onPartitionsAssigned {}", partitions);
        // onAssign methods may perform seek. It is safe to use the consumer here since we are in a poll()
        for (Consumer<Collection<SeekablePartition>> onAssign : assignListeners) {
            onAssign.accept(toSeekable(partitions));
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.debug("onPartitionsRevoked {}", partitions);
        // It is safe to use the consumer here since we are in a poll()
        doCommit(true);
        for (Consumer<Collection<SeekablePartition>> onRevoke : revokeListeners) {
            onRevoke.accept(toSeekable(partitions));
        }
    }

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if (commitListener != null && exception == null)
            commitListener.accept(offsets);
        if (commitErrorListener != null && exception != null)
            commitErrorListener.accept(exception);
    }

    @Override
    public void subscribe(Subscriber<? super CommittableRecord<K, V>> subscriber) {
        log.debug("subscribe");
        if (consumerFlux != null)
            throw new IllegalStateException("Already subscribed.");

        consumerFlux = Flux.merge(fluxList)
                           .doOnSubscribe(s -> {
                                   for (Flux<EventType> flux : periodicFluxList) {
                                       try {
                                           cancellations.add(flux.subscribe());
                                       } catch (Exception e) {
                                           log.error("Subscription to periodic flux failed", e);
                                           throw e;
                                       }
                                   }
                               })
                           .concatMap(eventType -> doEvent(eventType))
                           .doOnCancel(() -> cancel())
                           .doOnNext(record -> {
                                   if (commitFlux != null)
                                       onConsumed(record.consumerRecord());
                               });
        consumerFlux.subscribe(subscriber);
    }

    private void cancel() {
        log.debug("cancel {}", isRunning);
        if (isRunning.getAndSet(false)) {
            consumer.wakeup();
            consumerSemaphore.acquireUninterruptibly();
            try {
                try {
                    // in case consumer was not in poll at the time of wakeup
                    consumer.poll(0);
                } catch (WakeupException e) {
                    // ignore
                }
                doCommit(true);
                for (Cancellation cancellation : cancellations)
                    cancellation.dispose();
                consumer.close();
            } finally {
                consumerSemaphore.release();
            }
        }
    }

    private void commitAsync(ConsumerRecord<K, V> record) {
        onConsumed(record);
        emit(EventType.COMMIT);
    }

    private void commitSync(ConsumerRecord<K, V> record) {
        onConsumed(record);
        consumerSemaphore.acquireUninterruptibly();
        try {
            Map<TopicPartition, OffsetAndMetadata> offsetMap = doCommit(true);
            if (commitListener != null)
                commitListener.accept(offsetMap);
        } catch (Exception e) {
            if (commitErrorListener != null)
                commitErrorListener.accept(e);
            else
                throw e;
        } finally {
            consumerSemaphore.release();
        }
    }

    private void emit(EventType eventType) {
        log.trace("emit {}", eventType);
        submissionEmitter.emit(eventType);
    }

    protected KafkaConsumer<K, V> kafkaConsumer() {
        return consumer;
    }

    protected Flux<CommittableRecord<K, V>> doEvent(EventType eventType) {
        log.trace("doEvent {}", eventType);
        ConsumerRecords<K, V> consumerRecords = null;
        if (consumerSemaphore.tryAcquire()) {
            try {
                if (isRunning.get()) {
                    switch (eventType) {
                        case INIT:
                        case POLL:
                            consumerRecords = doPoll();
                            break;
                        case COMMIT:
                            doCommit(false);
                            break;
                        case HEARTBEAT:
                            consumerRecords = doHeartbeat();
                            break;
                        default:
                            throw new IllegalArgumentException("Unexpected event type: " + eventType);
                    }
                }
            } catch (WakeupException e) {
                log.debug("Wakeup exception", e);
            } finally {
                consumerSemaphore.release();
            }
        }
        return consumerRecords == null ? Flux.empty() :
            Flux.fromIterable(consumerRecords)
                .map(record -> new CommittableKafkaConsumerRecord(record));
    }

    private ConsumerRecords<K, V> doPoll() {
        ConsumerRecords<K, V> records;
        needsHeartbeat.set(false);
        records = consumer.poll(pollTimeout.toMillis());
        if (requestsPending.addAndGet(0 - records.count()) > 0)
            emit(EventType.POLL);
        return records;
    }

    private Map<TopicPartition, OffsetAndMetadata> doCommit(boolean sync) {
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
            if (sync)
                consumer.commitSync(offsetMap);
            else
                consumer.commitAsync(offsetMap, this);
        }
        return offsetMap;
    }

    private ConsumerRecords<K, V> doHeartbeat() {
        if (needsHeartbeat.getAndSet(true)) {
            consumer.pause(consumer.assignment());
            consumer.poll(0);
            consumer.resume(consumer.assignment());
        }
        return null;
    }

    private void onConsumed(ConsumerRecord<K, V> record) {
        consumedOffsets.put(new TopicPartition(record.topic(), record.partition()), record.offset());
    }

    private Collection<SeekablePartition> toSeekable(Collection<TopicPartition> partitions) {
        List<SeekablePartition> seekableList = new ArrayList<>(partitions.size());
        for (TopicPartition partition : partitions)
            seekableList.add(new SeekableKafkaPartition(partition));
        return seekableList;
    }

    private class CommittableKafkaConsumerRecord implements CommittableRecord<K, V> {

        private final ConsumerRecord<K, V> consumerRecord;

        CommittableKafkaConsumerRecord(ConsumerRecord<K, V> consumerRecord) {
            this.consumerRecord = consumerRecord;
        }

        @Override
        public ConsumerRecord<K, V> consumerRecord() {
            return consumerRecord;
        }

        @Override
        public void commitAsync() {
            KafkaFlux.this.commitAsync(consumerRecord);
        }

        @Override
        public void commitSync() {
            KafkaFlux.this.commitSync(consumerRecord);
        }

        @Override
        public String toString() {
            return String.valueOf(consumerRecord);
        }
    }

    private class SeekableKafkaPartition implements SeekablePartition {
        private final TopicPartition topicPartition;

        SeekableKafkaPartition(TopicPartition topicPartition) {
            this.topicPartition = topicPartition;
        }

        @Override
        public TopicPartition topicPartition() {
            return topicPartition;
        }

        @Override
        public void seekToBeginning() {
            consumer.seekToBeginning(Collections.singletonList(topicPartition));
        }

        @Override
        public void seekToEnd() {
            consumer.seekToEnd(Collections.singletonList(topicPartition));
        }

        @Override
        public void seek(long offset) {
            consumer.seek(topicPartition, offset);
        }

        @Override
        public long position(TopicPartition partition) {
            return consumer.position(partition);
        }
    }
}
