package reactor.kafka;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

import reactor.core.publisher.Mono;
import reactor.kafka.internals.Utils;

public class KafkaSender<K, V> {

    private final Mono<KafkaProducer<K, V>> producerMono;
    private final AtomicBoolean hasProducer = new AtomicBoolean();
    private ExecutorService callbackExecutor;

    public static <K, V> KafkaSender<K, V> create(KafkaContext<K, V> context) {
        return new KafkaSender<>(context);
    }

    public KafkaSender(KafkaContext<K, V> context) {
        this.producerMono = Mono.fromCallable(() -> {
                if (callbackExecutor == null)
                    callbackExecutor = Executors.newSingleThreadExecutor(Utils.newThreadFactory("reactor-sender"));
                return context.getProducerFactory().createProducer();
            })
            .cache()
            .doOnSubscribe(s -> hasProducer.set(true));
    }

    public Mono<RecordMetadata> send(ProducerRecord<K, V> record) {
        return producerMono
                     .then(producer -> doSend(producer, record).publishOn(callbackExecutor));
    }

    public Mono<List<PartitionInfo>> partitionsFor(String topic) {
        return producerMono
                .then(producer -> Mono.just(producer.partitionsFor(topic)));
    }

    public void setCallbackExecutor(ExecutorService callbackExecutor) {
        this.callbackExecutor = callbackExecutor;
    }

    public void close() {
        if (hasProducer.getAndSet(false))
            producerMono.get().close();
        if (callbackExecutor != null)
            callbackExecutor.shutdownNow();
    }

    private Mono<RecordMetadata> doSend(KafkaProducer<K, V> producer, ProducerRecord<K, V> record) {
        return Mono.create(emitter -> producer.send(record, (metadata, exception) -> {
                if (exception == null)
                    emitter.complete(metadata);
                else
                    emitter.fail(exception);
            }));
    }
}
