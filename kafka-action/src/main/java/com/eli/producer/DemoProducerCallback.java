package com.eli.producer;


import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Created by pekah on 2018/8/30.
 */
public class DemoProducerCallback implements Callback {
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if(e != null) {
            System.out.println("消息发送异常，异常内容" + e.getMessage());
            e.printStackTrace();
        } else {
            System.out.println("消息发送成功，消息内容为：" + JSON.toJSONString(recordMetadata));
        }
    }
}
