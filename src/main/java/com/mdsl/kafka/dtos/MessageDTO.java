package com.mdsl.kafka.dtos;

import com.mdsl.kafka.enums.Status;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MessageDTO {
    private String message;
    private Status status;
}
