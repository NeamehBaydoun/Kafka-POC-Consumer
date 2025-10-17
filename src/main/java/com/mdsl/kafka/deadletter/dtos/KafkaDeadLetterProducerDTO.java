package com.mdsl.kafka.deadletter.dtos;

import com.mdsl.kafka.dtos.DeadLetterMessageDTO;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaDeadLetterProducerDTO {
    private DeadLetterMessageDTO deadLetterMessageDTO;
    private String topic;
    private int partition;
}
