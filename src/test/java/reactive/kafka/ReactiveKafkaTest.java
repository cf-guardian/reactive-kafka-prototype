package reactive.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class ReactiveKafkaTest {
    
    private static final boolean USE_LOCK_FREE = true;

    private ReactiveKafka<String, String> reactiveKafka;
    private String topic = "testtopic";
    private int partition = 0;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setUp() {
        reactiveKafka = USE_LOCK_FREE ? new LockFreeReactiveKafka<>() : new ReactiveKafka<>();
    }

    @After
    public void tearDown() {
        reactiveKafka.close();
    }

    @Test
    public final void reactiveSendTest() {
        int count = 10;
        for (int i = 0; i < count; i++) {
            Mono<RecordMetadata> sendMono = reactiveKafka.send(topic, partition, "", testName.getMethodName());

            RecordMetadata metadata = sendMono.get(Duration.ofSeconds(10));

            assertEquals(partition, metadata.partition());
            assertTrue("Invalid offset " + metadata.offset(), metadata.offset() >= 0);
        }
    }

    @Test
    public final void reactiveConsumeAutoCommitTest() throws Exception {
        String groupId = testName.getMethodName() + ".1";
        Duration pollTimeout = Duration.ofMillis(5);
        Duration commitInterval = Duration.ofSeconds(1);
        Properties properties = new Properties();
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Flux<ConsumerRecord<String, String>> incomingFlux =
                reactiveKafka.receiveAutoCommit(groupId, Collections.singletonList(topic), pollTimeout, commitInterval, properties);

        int count = 10;
        CountDownLatch latch = new CountDownLatch(count);
        incomingFlux.doOnNext(record -> {
                         System.out.println("Received message: " + record);
                         latch.countDown();
                     })
                    .subscribeOn(Executors.newSingleThreadExecutor())
                    .subscribe();

        for (int i = 0; i < count; i++)
            reactiveKafka.send(topic, partition, "", testName.getMethodName() + ": Message " + i).get();

        assertTrue(latch.getCount() + " messages not received", latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public final void reactiveConsumeManualCommitTest() throws Exception {
        String groupId = testName.getMethodName() + ".1";
        Duration pollTimeout = Duration.ofMillis(5);
        Flux<CommittableConsumerRecord<String, String>> incomingFlux =
                reactiveKafka.receive(groupId, Collections.singletonList(topic), pollTimeout, null);

        int count = 10;
        CountDownLatch latch = new CountDownLatch(count);
        incomingFlux.doOnNext(record -> {
                         System.out.println("  Received message: " + record);
                         latch.countDown();
                         record.commitAsync();
                     })
                    .subscribeOn(Executors.newSingleThreadExecutor())
                    .subscribe();

        for (int i = 0; i < count; i++)
            reactiveKafka.send(topic, partition, "", testName.getMethodName() + ": Message " + i).get();

        assertTrue(latch.getCount() + " messages not received", latch.await(10, TimeUnit.SECONDS));
    }
}
