package com.mdsl.kafka.consumer;

import com.mdsl.kafka.dtos.DeadLetterMessageDTO;
import com.mdsl.kafka.dtos.MessageDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class MessageConsumer {

    @KafkaListener(
            topicPartitions = @TopicPartition(
                    topic = "string-topic", partitions = {"0"}),
            groupId = "consumer-group",
            containerFactory = "stringKafkaListenerContainerFactory")
    public void stringMessageListener(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("Consumed message from partition {}, message: {}", record.partition(), record.value());
        ack.acknowledge();
    }

    @KafkaListener(
            topicPartitions = @TopicPartition(
                    topic = "string-topic", partitions = {"1"}),
            groupId = "consumer-group",
            containerFactory = "bulkKafkaListenerContainerFactory"
    )
    public void bulkStringMessageListener(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        log.info("received {} records", records.size());
        for (ConsumerRecord<String, String> record : records) {
            log.info("{}: {}", Thread.currentThread().getId(), record.value());
        }
        ack.acknowledge();
    }

    @KafkaListener(
            topicPartitions = @TopicPartition(
                    topic = "dto-topic", partitions = {"0"}),
            groupId = "consumer-group",
            containerFactory = "dtoKafkaListenerContainerFactory")
    public void dtoMessageListener(ConsumerRecord<String, MessageDTO> record, Acknowledgment ack) {
        log.info("consumed");
        log.info("Consumed message from partition {}, message : {}, status : {}", record.partition(), record.value().getMessage(), record.value().getStatus());
        ack.acknowledge();
    }

    @KafkaListener(
            topicPartitions = @TopicPartition(
                    topic = "dto-topic", partitions = {"1"}),
            groupId = "consumer-group",
            containerFactory = "bulkDTOKafkaListenerContainerFactory"
    )
    public void bulkDTOMessageListener(List<ConsumerRecord<String, MessageDTO>> records, Acknowledgment ack) {
        log.info("received {} records", records.size());
        for (ConsumerRecord<String, MessageDTO> record : records) {
            log.info("{}: message: {}, status: {}", Thread.currentThread().getId(), record.value().getMessage(), record.value().getStatus());
        }
        ack.acknowledge();
    }

    @KafkaListener(
            topicPartitions = @TopicPartition(
                    topic = "dead-letter-topic", partitions = {"0"}),
            groupId = "consumer-group",
            containerFactory = "deadLetterDtoKafkaListenerContainerFactory")
    public void deadLetterDtoMessageListener(ConsumerRecord<String, DeadLetterMessageDTO> record, Acknowledgment ack) {
        log.info("Consumed dead letter message from topic {}, partition {}, error : {}, status : {}", record.value().getTopic(), record.partition(), record.value().getError(), record.value().getStatus());
        //save in db
        ack.acknowledge();
    }
}
