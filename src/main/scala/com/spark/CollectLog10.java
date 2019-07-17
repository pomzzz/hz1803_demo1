package com.spark;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.Properties;

public class CollectLog10 {
    public static void main(String[] args) {
        // 这个是用来配置kafka的参数
        Properties prop = new Properties();
        // 这里不是配置broker.id 了，这个事配置bootstrap.servers
        prop.put("bootstrap.servers","192.168.47.160:9092,192.168.47.161:9092,192.168.47.162:9092,");
        // 下面是配置key和value的序列化
        prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(prop);
        try {
            // 路径
            BufferedReader bf = new BufferedReader(new FileReader(new File("E:\\大数据学习资料\\cdh\\项目（二）01\\充值平台实时统计分析\\cmcc.json")));
            String line = null;
            while((line=bf.readLine())!= null){
                Thread.sleep(1000);
                producer.send(new ProducerRecord<String,String>("socend",line));
            }
            // 关闭
            bf.close();
            producer.close();
            System.out.println("已经发送完毕");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
