package com.eli.provider;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by pekah on 2018/8/30.
 */
public class Producer {
    public static void main(String[] args) throws Exception{
        // 初始化生产者配置
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer kafkaProducer = new KafkaProducer<String, String>(kafkaProps);

        // 发送消息
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("topic", "test message1");

        // 方式一：直接发送消息，忽略消息发送成功失败异常的结果
//        kafkaProducer.send(record);

        // 方式二：同步发送消息，直到消息发送成功或者抛出异常
//        kafkaProducer.send(record).get();

        // 方式三：异步发送消息，有消息结果时会回调设置的回调函数
        kafkaProducer.send(record, new DemoProducerCallback());

        Thread.currentThread().sleep(10000);

    }
}
