package ros.hack.bonuses.service.impl;

import com.github.voteva.Operation;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ros.hack.bonuses.config.KafkaProperties;
import ros.hack.bonuses.service.ConsumerService;
import ros.hack.bonuses.service.ProducerService;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import static ros.hack.bonuses.consts.Constants.*;
import static ros.hack.bonuses.utils.JsonParser.parse;

@Slf4j
@RequiredArgsConstructor
@Service
public class ConsumerServiceImpl implements ConsumerService<String, String> {

    private final KafkaProperties kafkaProperties;
    private final ProducerService producerService;

    @Override
    @Transactional
    @KafkaListener(topics = "${kafka.payment-topic}",
            containerFactory = "kafkaListenerContainerFactory",
            groupId = "${kafka.group-id}")
    public void consume(@NonNull ConsumerRecord<String, String> consumerRecord) {
        log.info(consumerRecord.toString());
        producerService.send(kafkaProperties.getOperationTopic(), addCashback(parse(consumerRecord.value())));
    }

    @SneakyThrows(NullPointerException.class)
    private Operation addCashback(@NonNull Operation operation) {
        com.github.voteva.Service bonusService = new com.github.voteva.Service();
        if (operation.getServices() != null
                && operation.getServices().get(SERVICE_NAME) != null) {
            bonusService = operation.getServices().get(SERVICE_NAME);
        }

        Map<String, String> request = new HashMap<>();
        if (bonusService.getRequest() != null) {
            request = bonusService.getRequest();
        }
        Map<String, String> response = request;

        if (request.get(EXTENDED_STATUS).equals(OUT_STATUS)) {
            BigDecimal initialAmount = new BigDecimal(request.get(AMOUNT));
            response.put(CASHBACK, getRandomBonus(initialAmount).toString());
        }
        bonusService.setRequest(request);
        bonusService.setResponse(response);

        operation.getServices().
                put(SERVICE_NAME, bonusService);
        return operation;
    }

    public BigDecimal getRandomBonus(BigDecimal initialAmount) {
        return initialAmount.remainder(new BigDecimal(10)).add(new BigDecimal("7"));
    }
}
