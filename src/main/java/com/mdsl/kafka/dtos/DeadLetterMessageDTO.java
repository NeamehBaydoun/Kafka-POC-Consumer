package com.mdsl.kafka.dtos;

import com.mdsl.kafka.enums.Status;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DeadLetterMessageDTO {
    private String error;
    private Status status;
    private String topic;
    private int partition;
}
