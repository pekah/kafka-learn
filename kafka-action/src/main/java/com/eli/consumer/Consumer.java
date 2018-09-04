package com.eli.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * Created by elizhou on 2018/9/4.
 */
public class Consumer {
    private static KafkaConsumer consumer;

    public static void main(String[] args) {
        // 初始化消费者配置
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("group.id", "test"); // 消费者组
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 设置不自动提交偏移量
        kafkaProps.put("enable.auto.commit", "false");

        consumer = new KafkaConsumer(kafkaProps);

        try {
            consumer.subscribe(Collections.singletonList("topic"), new HandleRebalance());

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("offset:" + record.offset() + ", key:" + record.key() + ", value:" + record.value());
                }

                // 每消费完一批消息，异步提交消息偏移量
                consumer.commitAsync();
            }
        } catch (WakeupException e) {
            e.printStackTrace();
        } finally {
            try {
                // 抛出异常，在关闭consumer前，同步提交消息偏移量
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }


    }

    private static class HandleRebalance implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {

        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            // 在消费者失去分区所有权之前同步提交最新的消息偏移量
            consumer.commitSync();
        }
    }
}


