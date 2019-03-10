package com.window;

import com.alibaba.fastjson.JSON;
import com.entity.CarEntity;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
import java.util.Random;


public class TestProducer {

    public static void main(String[] args) {

        Properties props = new Properties();

        //broker地址
        props.put("bootstrap.servers", "localhost:9092");

        //请求时候需要验证
        props.put("acks", "all");

        //请求失败时候需要重试
        props.put("retries", 0);

        //内存缓存区大小
        props.put("buffer.memory", 33554432);

        //指定消息key序列化方式
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        //指定消息本身的序列化方式
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);




        while (true){

            CarEntity carEntity=new CarEntity();
            Random random=new Random();
            carEntity.setCarKind(random.nextInt(3)+"");
            carEntity.setTimeStamp(System.currentTimeMillis());
            carEntity.setCarSum(random.nextInt(3));

            String result = JSON.toJSONString(carEntity);

            producer.send(new ProducerRecord<>("carwindow", result, result));
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }

}
