package com.zavier.kafka;

import com.zavier.kafka.interceptor.DemoProducerInterceptor;
import com.zavier.kafka.partition.DemoPartitioner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Properties;
import java.util.stream.IntStream;

@Slf4j
public class Producer {
    public static final String brokerList = "47.93.214.100:9092";
    public static final String topic = "topic-demo";

    public static Properties initConfig() {
        final Properties properties = new Properties();
        // kafka broker地址列表
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        // 消息key的序列化器,消息发送是必须转为字节数组  （可以实现Serializer实现自己自定的序列化器）
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 消息value(内容)的序列化器
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 发送失败后重试最大次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);
        // 指定自定义分区器，不指定则默认为DefaultPartitioner
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DemoPartitioner.class.getName());
        // 指定自定义拦截器，多个拦截器以逗号分隔，默认为空
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, DemoProducerInterceptor.class.getName());
        return properties;
    }

    /**
     * 发送过程 拦截器->序列化器->分区器->消息累加器(用于缓存消息给sender线程调用发送)
     * @param args
     */
    public static void main(String[] args) {
        final Properties properties = initConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        IntStream.range(0, 10)
                .forEach(e -> {
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, "Hello, Kafka!");
                    // producer.send为异步方法
                    producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                        if (exception != null) {
                            log.error("send record error", exception);
                        } else {
                            log.info("send success! topic:{} - partition:{} - offset:{}", metadata.topic(), metadata.partition(), metadata.offset());
                        }
                    });
                });
        producer.close(Duration.ofSeconds(10));
    }
}
