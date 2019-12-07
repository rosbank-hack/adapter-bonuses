package ros.hack.bonuses.config;

import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
@ConditionalOnBean(KafkaProperties.class)
public class KafkaProducerConfiguration {

    private final KafkaProperties properties;

    @Bean
    public DefaultKafkaProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServersConfig());
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, properties.getMaxBlockMsConfig());
        configProps.put(ProducerConfig.RETRIES_CONFIG, properties.getRetriesConfig());
        configProps.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, properties.getReconnectBackoffMsConfig());
        configProps.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, properties.getReconnectBackoffMaxMsConfig());
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, properties.getBatchSizeConfig());
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
