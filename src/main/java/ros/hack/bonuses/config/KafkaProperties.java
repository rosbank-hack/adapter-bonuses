package ros.hack.bonuses.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotBlank;

@Data
@Validated
@ConfigurationProperties("kafka")
public class KafkaProperties {

    @NotBlank
    private String operationTopic;

    @NotBlank
    private String paymentTopic;

    @NotBlank
    private String groupId;

    @NotBlank
    private String bootstrapServersConfig;

    @NotBlank
    private String maxBlockMsConfig;

    @NotBlank
    private String retriesConfig;

    @NotBlank
    private String reconnectBackoffMsConfig;

    @NotBlank
    private String reconnectBackoffMaxMsConfig;

    @NotBlank
    private String batchSizeConfig;
}
