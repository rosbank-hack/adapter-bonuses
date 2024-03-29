package ros.hack.bonuses.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.voteva.Operation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ros.hack.bonuses.service.ProducerService;

@Slf4j
@RequiredArgsConstructor
@Service
public class ProducerServiceImpl implements ProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Override
    @Transactional
    public void send(String topic, Operation operation) {
        try {
            log.debug(operation.toString());
            kafkaTemplate.send(topic, objectMapper.writeValueAsString(operation));
        } catch (JsonProcessingException e) {
            log.error("Json processing exception: " + e);
        }
    }
}
