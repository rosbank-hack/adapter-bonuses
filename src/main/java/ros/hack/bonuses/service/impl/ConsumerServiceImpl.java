package ros.hack.bonuses.service.impl;

import com.github.voteva.Operation;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ros.hack.bonuses.service.ConsumerService;
import ros.hack.bonuses.service.ProducerService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ros.hack.bonuses.consts.BonusConsts.SERVICE_NAME;

@Slf4j
@RequiredArgsConstructor
@Service
public class ConsumerServiceImpl implements ConsumerService {

    @Value("${kafka.topic.operation-topic}")
    private String topic;

    private final ProducerService producerService;

    @Override
    @Transactional
    @KafkaListener(topics = "${kafka.topic.payment-topic}", containerFactory = "kafkaListenerContainerFactory")
    public void consume(@NonNull List<Operation> operations) {
        operations.forEach(operation -> {
            log.info(operation.toString());
            producerService.send(topic, addCashback(operation));
        });
    }

    private Operation addCashback(@NonNull Operation operation) {
        Map<String, com.github.voteva.Service> services = new HashMap<>();
        Map<String, String> response = operation
                .getServices()
                .get(SERVICE_NAME)
                .getRequest();
        response.put("amount", String.valueOf(Math.random()));

        com.github.voteva.Service bonusService = com.github.voteva.Service.builder()
                .request(operation
                        .getServices()
                        .get(SERVICE_NAME)
                        .getRequest())
                .response(response)
                .build();
        services.put(SERVICE_NAME, bonusService);
        operation.setServices(services);
        return operation;
    }
}
