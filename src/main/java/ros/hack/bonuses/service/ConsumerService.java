package ros.hack.bonuses.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerService<K, V> {
    void consume(ConsumerRecord<K, V> items);
}