package reactor.kafka;

import java.time.Duration;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import reactor.core.flow.Cancellation;
import reactor.core.publisher.Flux;
import reactor.kafka.KafkaFlux.EventType;
import reactor.kafka.util.TestUtils;

public class InboundTest extends AbstractKafkaTest {

    private KafkaSender<Integer, String> kafkaSender;

    private ExecutorService consumerExecutor;
    private String groupId;
    private Semaphore assignSemaphore = new Semaphore(0);
    private List<Cancellation> subscribeCancellations = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        groupId = testName.getMethodName();
        kafkaSender = new KafkaSender<>(outboundKafkaContext);
        consumerExecutor = Executors.newCachedThreadPool(TestUtils.newThreadFactory("test-consumer"));
    }

    @After
    public void tearDown() {
        for (Cancellation cancellation : subscribeCancellations)
            cancellation.dispose();
        kafkaSender.close();
        consumerExecutor.shutdown();
    }

    @Test
    public final void autoCommitTest() throws Exception {
        TestableKafkaFlux testableKafkaFlux = TestableKafkaFlux.create(inboundKafkaContext, groupId, Collections.singletonList(topic));
        Flux<CommittableRecord<Integer, String>> incomingFlux = testableKafkaFlux
                         .doOnPartitionsAssigned(this::onPartitionsAssigned)
                         .autoCommit(Duration.ofMillis(50));
        consumeAndCheck(incomingFlux, 0, 0, 100, 0, 100);
        TestUtils.waitUntil("No auto commits", f -> testableKafkaFlux.count(EventType.COMMIT) > 0, testableKafkaFlux, Duration.ofMillis(1000));
    }

    @Test
    public final void manualCommitTest() throws Exception {
        Flux<CommittableRecord<Integer, String>> incomingFlux =
                KafkaFlux.listenOn(inboundKafkaContext, groupId, Collections.singletonList(topic))
                         .doOnPartitionsAssigned(this::onPartitionsAssigned)
                         .doOnNext(record -> record.commitAsync());
        consumeAndCheck(incomingFlux, 0, 0, 100, 0, 100);
    }

    @Test
    public final void commitCallbackTest() throws Exception {
        int count = 10;
        CountDownLatch commitLatch = new CountDownLatch(count);
        long[] committedOffsets = new long[partitions];
        Flux<CommittableRecord<Integer, String>> incomingFlux =
                KafkaFlux.listenOn(inboundKafkaContext, groupId, Collections.singletonList(topic))
                         .doOnPartitionsAssigned(this::seekToBeginning)
                         .doOnCommit(offsetMap -> onCommit(offsetMap, commitLatch, committedOffsets))
                         .doOnNext(record -> record.commitAsync());

        subscribe(incomingFlux, new CountDownLatch(count));
        sendMessages(0, count);
        checkCommitCallbacks(commitLatch, committedOffsets);
    }

    @Test
    public final void commitFailureTest() throws Exception {
        int count = 1;

        Semaphore revokeSemaphore = new Semaphore(0);
        Semaphore commitErrorSemaphore = new Semaphore(0);
        Flux<CommittableRecord<Integer, String>> incomingFlux =
                KafkaFlux.listenOn(inboundKafkaContext, groupId, Collections.singletonList(topic))
                         .doOnPartitionsAssigned(this::seekToBeginning)
                         .doOnPartitionsRevoked(p -> revokeSemaphore.release())
                         .doOnCommitError(t -> commitErrorSemaphore.release())
                         .doOnCommit(offsets -> fail("Unexpected commit success"))
                         .doOnNext(record -> {
                                 deleteTopic(topic);
                                 revokeSemaphore.acquireUninterruptibly();
                                 record.commitAsync();
                             })
                         .doOnError(e -> e.printStackTrace());

        subscribe(incomingFlux, new CountDownLatch(count));
        sendMessages(0, count);
        assertTrue("Commit error callback not invoked", commitErrorSemaphore.tryAcquire(receiveTimeoutMillis, TimeUnit.MILLISECONDS));
    }

    @Test
    public final void commitSyncTest() throws Exception {
        int count = 10;
        CountDownLatch commitLatch = new CountDownLatch(count);
        long[] committedOffsets = new long[partitions];
        for (int i = 0; i < committedOffsets.length; i++)
            committedOffsets[i] = -1;
        Flux<CommittableRecord<Integer, String>> incomingFlux =
                KafkaFlux.listenOn(inboundKafkaContext, groupId, Collections.singletonList(topic))
                         .doOnPartitionsAssigned(this::onPartitionsAssigned)
                         .doOnCommit(offsetMap -> onCommit(offsetMap, commitLatch, committedOffsets))
                         .doOnNext(record -> {
                                 assertEquals(committedOffsets[record.consumerRecord().partition()] + 1, record.consumerRecord().offset());
                                 record.commitSync();
                             })
                         .doOnError(e -> e.printStackTrace());

        subscribe(incomingFlux, new CountDownLatch(count));
        sendMessages(0, count);
        checkCommitCallbacks(commitLatch, committedOffsets);
    }

    @Test
    public final void seekToBeginningTest() throws Exception {
        int count = 10;
        sendMessages(0, count);
        KafkaFlux<Integer, String> incomingFlux =
                KafkaFlux.listenOn(inboundKafkaContext, groupId, Collections.singletonList(topic))
                         .doOnPartitionsAssigned(this::seekToBeginning)
                         .autoCommit(Duration.ofMillis(50));

        consumeAndCheck(incomingFlux, 0, count, count, 0, count * 2);
    }

    @Test
    public final void seekToEndTest() throws Exception {
        int count = 10;
        sendMessages(0, count);
        KafkaFlux<Integer, String> incomingFlux =
                KafkaFlux.listenOn(inboundKafkaContext, groupId, Collections.singletonList(topic))
                         .doOnPartitionsAssigned(partitions -> {
                                 for (SeekablePartition partition : partitions)
                                     partition.seekToEnd();
                                 onPartitionsAssigned(partitions);
                             })
                         .autoCommit(Duration.ofMillis(50));

        consumeAndCheck(incomingFlux, 100, count, count, count, count);
    }

    @Test
    public final void seekTest() throws Exception {
        int count = 10;
        sendMessages(0, count);
        Flux<CommittableRecord<Integer, String>> incomingFlux =
                KafkaFlux.listenOn(inboundKafkaContext, groupId, Collections.singletonList(topic))
                         .doOnPartitionsAssigned(partitions -> {
                                 onPartitionsAssigned(partitions);
                                 for (SeekablePartition partition : partitions)
                                     partition.seek(1);
                             })
                         .autoCommit(Duration.ofMillis(50))
                         .doOnError(e -> e.printStackTrace());

        consumeAndCheck(incomingFlux, 0, count, count, partitions, count * 2 - partitions);
    }

    @Test
    public final void wildcardSubscribeTest() throws Exception {
        KafkaFlux<Integer, String> incomingFlux =
                KafkaFlux.listenOn(inboundKafkaContext, groupId, Pattern.compile("test.*"))
                         .doOnPartitionsAssigned(this::onPartitionsAssigned)
                         .autoCommit(Duration.ofMillis(50));
        consumeAndCheck(incomingFlux, 0, 0, 10, 0, 10);
    }

    @Test
    public final void manualAssignmentTest() throws Exception {
        Flux<CommittableRecord<Integer, String>> incomingFlux =
                KafkaFlux.assign(inboundKafkaContext, groupId, getTopicPartitions())
                         .autoCommit(Duration.ofMillis(50))
                         .doOnSubscribe(s -> assignSemaphore.release());
        consumeAndCheck(incomingFlux, 500, 0, 10, 0, 10);
    }

    @Test
    public final void transferMessagesTest() throws Exception {
        int count = 10;
        CountDownLatch sendLatch0 = new CountDownLatch(count);
        CountDownLatch sendLatch1 = new CountDownLatch(count);
        CountDownLatch receiveLatch0 = new CountDownLatch(count);
        CountDownLatch receiveLatch1 = new CountDownLatch(count);
        // Subscribe on partition 1
        Flux<CommittableRecord<Integer, String>> partition1Flux =
                KafkaFlux.assign(inboundKafkaContext, "group2", Collections.singletonList(new TopicPartition(topic, 1)))
                         .doOnPartitionsAssigned(this::seekToBeginning)
                         .autoCommit(Duration.ofMillis(50))
                         .doOnPartitionsAssigned(this::onPartitionsAssigned);
        subscribe(partition1Flux, receiveLatch1);

        // Receive from partition 0 and send to partition 1
        Cancellation cancellation0 =
            KafkaFlux.assign(inboundKafkaContext, groupId, Collections.singletonList(new TopicPartition(topic, 0)))
                     .doOnPartitionsAssigned(this::seekToBeginning)
                     .concatMap(record -> {
                             receiveLatch0.countDown();
                             return kafkaSender.send(new ProducerRecord<Integer, String>(topic, 1, record.consumerRecord().key(), record.consumerRecord().value()))
                                               .doOnSuccess(sendResult -> {
                                                       record.commitSync();
                                                       sendLatch1.countDown();
                                                   });
                         })
                     .doOnError(e -> e.printStackTrace())
                     .subscribeOn(consumerExecutor)
                     .subscribe();
        subscribeCancellations.add(cancellation0);

        // Send messages to partition 0
        Flux.range(0, count)
            .flatMap(i -> kafkaSender.send(new ProducerRecord<Integer, String>(topic, 0, i, "Message " + i))
                                     .doOnSuccess(metadata -> sendLatch0.countDown()))
            .subscribe();

        if (!sendLatch0.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS))
            fail(sendLatch0.getCount() + " messages not sent to partition 0");
        waitForMessages(receiveLatch0);
        if (!sendLatch1.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS))
            fail(sendLatch1.getCount() + " messages not sent to partition 1");

        // Check messages received on partition 1
        waitForMessages(receiveLatch1);
    }

    @Test
    public final void heartbeatTest() throws Exception {
        int count = 5;
        this.receiveTimeoutMillis = sessionTimeoutMillis * 10;
        AtomicInteger revoked = new AtomicInteger();
        TestableKafkaFlux testableKafkaFlux = TestableKafkaFlux.create(inboundKafkaContext, groupId, Collections.singletonList(topic));
        Flux<CommittableRecord<Integer, String>> incomingFlux = testableKafkaFlux
                         .doOnPartitionsRevoked(partitions -> revoked.addAndGet(partitions.size()))
                         .doOnPartitionsAssigned(this::onPartitionsAssigned)
                         .useCapacity(2)
                         .doOnNext(record -> TestUtils.sleep(sessionTimeoutMillis + 1000));

        consumeAndCheck(incomingFlux, 0, 0, count, 0, count);
        assertEquals(0, revoked.get());
        assertTrue("Heartbeats not sent: " + testableKafkaFlux.events, testableKafkaFlux.count(EventType.HEARTBEAT) > 1);
    }

    @Test
    public final void brokerRestartTest() throws Exception {
        int count = 10;
        CountDownLatch sendLatch = new CountDownLatch(count);
        Semaphore revokedSemapore = new Semaphore(0);
        AtomicInteger received = new AtomicInteger();
        Flux<CommittableRecord<Integer, String>> incomingFlux =
                KafkaFlux.listenOn(inboundKafkaContext, groupId, Collections.singletonList(topic))
                         .doOnPartitionsRevoked(p -> revokedSemapore.release())
                         .doOnPartitionsAssigned(this::onPartitionsAssigned)
                         .autoCommit(Duration.ofMillis(50))
                         .useCapacity(2)
                         .doOnNext(record -> {
                                 try {
                                     if (received.getAndIncrement() == 0) {
                                         sendLatch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS);
                                         embeddedKafka.bounce(0);
                                         TestUtils.sleep(2000);
                                         embeddedKafka.restart(0);
                                     }
                                 } catch (Exception e) {
                                     throw new RuntimeException(e);
                                 }
                             })
                         .doOnError(e -> e.printStackTrace());

        CountDownLatch latch = new CountDownLatch(count);
        subscribe(incomingFlux, latch);
        sendMessages(0, count, sendLatch);
        assertTrue("Revoked callback not invoked during broker restart", revokedSemapore.tryAcquire(receiveTimeoutMillis, TimeUnit.MILLISECONDS));
        receiveTimeoutMillis += 10000;
        waitForMessages(latch);
        checkConsumedMessages();
    }

    @Test
    public final void closeTest() throws Exception {
        int count = 10;
        for (int i = 0; i < 2; i++) {
            TestableKafkaFlux testableKafkaFlux = TestableKafkaFlux.create(inboundKafkaContext, groupId, Collections.singletonList(topic));
            Flux<CommittableRecord<Integer, String>> incomingFlux = testableKafkaFlux
                             .doOnPartitionsAssigned(this::onPartitionsAssigned)
                             .autoCommit(Duration.ofMillis(50));

            CountDownLatch latch = new CountDownLatch(count);
            Cancellation cancellation = subscribe(incomingFlux, latch);
            sendMessages(0, count);
            assertTrue(latch.getCount() + " messages not received", latch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS));
            cancellation.dispose();
            try {
                testableKafkaFlux.kafkaConsumer().partitionsFor(topic);
                fail("Consumer not closed");
            } catch (IllegalStateException e) {
                // expected exception
            }
        }
    }

    @Test
    public final void multiConsumerTest() throws Exception {
        int count = 100;
        CountDownLatch latch = new CountDownLatch(count);
        @SuppressWarnings("unchecked")
        Flux<CommittableRecord<Integer, String>>[] incomingFlux = new Flux[partitions];
        AtomicInteger[] receiveCount = new AtomicInteger[partitions];
        for (int i = 0; i < partitions; i++) {
            final int id = i;
            receiveCount[i] = new AtomicInteger();
            incomingFlux[i] =
                KafkaFlux.listenOn(inboundKafkaContext, groupId, Collections.singletonList(topic))
                         .doOnPartitionsAssigned(this::onPartitionsAssigned)
                         .autoCommit(Duration.ofMillis(50))
                         .doOnNext(record -> {
                                 receiveCount[id].incrementAndGet();
                                 onReceive(record.consumerRecord());
                                 latch.countDown();
                             })
                         .doOnError(e -> e.printStackTrace())
                         .subscribeOn(consumerExecutor);
            subscribeCancellations.add(incomingFlux[i].subscribe());
            assignSemaphore.acquire();
        }
        sendMessages(0, count);
        waitForMessages(latch);
        checkConsumedMessages(0, count);
    }

    @Test
    public void backPressureTest() throws Exception {
        int count = 10;
        Semaphore blocker = new Semaphore(0);
        Semaphore receiveSemaphore = new Semaphore(0);
        AtomicInteger receivedCount = new AtomicInteger();
        TestableKafkaFlux testableKafkaFlux = TestableKafkaFlux.create(inboundKafkaContext, groupId, Collections.singletonList(topic));
        Flux<CommittableRecord<Integer, String>> incomingFlux = testableKafkaFlux
                         .doOnPartitionsAssigned(this::onPartitionsAssigned)
                         .useCapacity(2)
                         .doOnNext(record -> {
                                 if (receivedCount.getAndIncrement() == 0)
                                     testableKafkaFlux.events.clear();
                                 receiveSemaphore.release();
                                 try {
                                     blocker.acquire();
                                 } catch (InterruptedException e) {
                                     throw new RuntimeException(e);
                                 }
                             })
                         .doOnError(e -> e.printStackTrace());

        subscribe(incomingFlux, new CountDownLatch(1));
        sendMessages(0, count);

        TestUtils.sleep(2000);
        assertTrue("Message not received", receiveSemaphore.tryAcquire(receiveTimeoutMillis, TimeUnit.MILLISECONDS));
        assertEquals(1, receivedCount.get());
        assertEquals(0, testableKafkaFlux.count(EventType.POLL));
        long endTimeMillis = System.currentTimeMillis() + receiveTimeoutMillis;
        while (receivedCount.get() < count && System.currentTimeMillis() < endTimeMillis) {
            blocker.release();
            assertTrue("Message not received " + receivedCount, receiveSemaphore.tryAcquire(requestTimeoutMillis, TimeUnit.MILLISECONDS));
            int pollCount = testableKafkaFlux.events.size();
            Thread.sleep(10);
            assertEquals(pollCount, testableKafkaFlux.events.size());
        }
    }

    private void waitForMessages(CountDownLatch latch) throws InterruptedException {
        if (!latch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS))
            fail(latch.getCount() + " messages not received, received=" + receivedMessages);
    }

    private void consumeAndCheck(Flux<CommittableRecord<Integer, String>> incomingFlux,
            long sendDelayMs, int sendStartIndex, int sendCount,
            int receiveStartIndex, int receiveCount) throws Exception {

        CountDownLatch latch = new CountDownLatch(receiveCount);
        subscribe(incomingFlux, latch);
        if (sendCount > 0) {
            if (sendDelayMs > 0) Thread.sleep(sendDelayMs);
            sendMessages(sendStartIndex, sendCount);
        }

        waitForMessages(latch);
        checkConsumedMessages(receiveStartIndex, receiveCount);
    }

    private Cancellation subscribe(Flux<CommittableRecord<Integer, String>> incomingFlux, CountDownLatch latch) throws Exception {
        Cancellation cancellation =
                incomingFlux
                        .doOnNext(record -> {
                                onReceive(record.consumerRecord());
                                latch.countDown();
                            })
                        .doOnError(e -> e.printStackTrace())
                        .subscribeOn(consumerExecutor)
                        .subscribe();
        subscribeCancellations.add(cancellation);
        assertTrue("Partitions not assigned", assignSemaphore.tryAcquire(requestTimeoutMillis, TimeUnit.MILLISECONDS));
        return cancellation;
    }

    private void sendMessages(int startIndex, int count) throws Exception {
        Flux.range(startIndex, count)
            .map(i -> createProducerRecord(i, true))
            .concatMap(record -> kafkaSender.send(record))
            .subscribe();
    }

    private void sendMessages(int startIndex, int count, CountDownLatch latch) throws Exception {
        Flux.range(startIndex, count)
            .map(i -> createProducerRecord(i, true))
            .concatMap(record -> kafkaSender.send(record)
                                            .doOnSuccess(metadata -> latch.countDown()))
            .subscribe();
    }

    private void onPartitionsAssigned(Collection<SeekablePartition> partitions) {
        assertEquals(topic, partitions.iterator().next().topicPartition().topic());
        assignSemaphore.release();
    }

    private void seekToBeginning(Collection<SeekablePartition> partitions) {
        for (SeekablePartition partition : partitions)
            partition.seekToBeginning();
        assertEquals(topic, partitions.iterator().next().topicPartition().topic());
        assignSemaphore.release();
    }

    private void onCommit(Map<TopicPartition, OffsetAndMetadata> offsetMap, CountDownLatch commitLatch, long[] committedOffsets) {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsetMap.entrySet()) {
            committedOffsets[entry.getKey().partition()] = entry.getValue().offset();
            commitLatch.countDown();
        }
    }

    private void checkCommitCallbacks(CountDownLatch commitLatch, long[] committedOffsets) throws InterruptedException {
        assertTrue(commitLatch.getCount() + " commit callbacks not invoked", commitLatch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS));
        for (int i = 0; i < partitions; i++)
            assertEquals(committedOffsets[i], receivedMessages.get(i).size() - 1);
    }

    private static class TestableKafkaFlux extends KafkaFlux<Integer, String> {

        private Map<Date, EventType> events = new ConcurrentSkipListMap<>();

        public static TestableKafkaFlux create(KafkaContext<Integer, String> context, String groupId, Collection<String> topics) {
            Consumer<KafkaFlux<Integer, String>> subscriber = (flux) -> flux.kafkaConsumer().subscribe(topics, flux);
            return new TestableKafkaFlux(context, subscriber, groupId);
        }

        public TestableKafkaFlux(KafkaContext<Integer, String> context, Consumer<KafkaFlux<Integer, String>> kafkaSubscriber, String groupId) {
            super(context, kafkaSubscriber, groupId);
        }

        @Override
        protected Flux<CommittableRecord<Integer, String>> doEvent(EventType eventType) {
            events.put(new Date(), eventType);
            return super.doEvent(eventType);
        }

        int count(EventType event) {
            int count = 0;
            for (EventType e : events.values()) {
                if (event == e)
                    count++;
            }
            return count;
        }
    }
}
