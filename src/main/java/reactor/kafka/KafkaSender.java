package reactor.kafka;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoEmitter;

public class KafkaSender<K, V> {

    private final Mono<KafkaProducer<K, V>> producerMono;
    private final AtomicBoolean hasProducer = new AtomicBoolean();

    public static <K, V> KafkaSender<K, V> create(KafkaContext<K, V> context) {
        return new KafkaSender<>(context);
    }

    public KafkaSender(KafkaContext<K, V> context) {
        this.producerMono = Mono.fromCallable(() -> context.getProducerFactory().createProducer()).cache().doOnSubscribe(s -> hasProducer.set(true));
    }

    public Mono<RecordMetadata> send(ProducerRecord<K, V> record) {
        return producerMono
                     .then(producer -> doSend(producer, record));
    }

    public Mono<List<PartitionInfo>> partitionsFor(String topic) {
        return producerMono
                .then(producer -> Mono.just(producer.partitionsFor(topic)));
    }

    public void close() {
        if (hasProducer.getAndSet(false))
            producerMono.get().close();
    }

    private Mono<RecordMetadata> doSend(KafkaProducer<K, V> producer, ProducerRecord<K, V> record) {
        return Mono.create(c -> producer.send(record, new SendCallback(c)));
    }

    private static class SendCallback implements Callback {
        private MonoEmitter<RecordMetadata> monoEmitter;
        SendCallback(MonoEmitter<RecordMetadata> monoEmitter) {
            this.monoEmitter = monoEmitter;
        }
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception == null)
                monoEmitter.complete(metadata);
            else
                monoEmitter.fail(exception);
        }
    }

}
