package ros.hack.bonuses.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value(value = "${kafka.bootstrap.servers.config}")
    private String bootstrapServersConfig;

    @Value(value = "${kafka.max-block-ms-config}")
    private String maxBlockMsConfig;

    @Value(value = "${kafka.retries-config}")
    private String retriesConfig;

    @Value(value = "${kafka.reconnect-backoff-ms-config}")
    private String reconnectBackoffMsConfig;

    @Value(value = "${kafka.reconnect-backoff-max-ms-config}")
    private String reconnectBackoffMaxMsConfig;

    @Value(value = "${kafka.batch-size-config}")
    private String batchSizeConfig;


    @Bean
    public DefaultKafkaProducerFactory<String, String> producerFactory() {

        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersConfig);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMsConfig);
        configProps.put(ProducerConfig.RETRIES_CONFIG, retriesConfig);
        configProps.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, reconnectBackoffMsConfig);
        configProps.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, reconnectBackoffMaxMsConfig);
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSizeConfig);
        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(configProps);
        producerFactory.setTransactionIdPrefix("tx");
        return producerFactory;

    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public KafkaTransactionManager kafkaTransactionManager(DefaultKafkaProducerFactory<String, String> producerFactory) {
        KafkaTransactionManager<String, String> ktm = new KafkaTransactionManager<>(producerFactory);
        ktm.setTransactionSynchronization(AbstractPlatformTransactionManager.SYNCHRONIZATION_ON_ACTUAL_TRANSACTION);
        return ktm;
    }
}
