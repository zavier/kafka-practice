package com.zavier.kafka.producer.interceptor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 生产者拦截器
 *
 */
@Slf4j
public class DemoProducerInterceptor implements ProducerInterceptor<String, String> {

    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;

    /**
     * 在对消息进行序列化及分区之前调用
     * @param record
     * @return
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String modifiedValue = "prefix1-" + record.value();
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(),
                record.key(), modifiedValue, record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            sendSuccess++;
        } else {
            sendFailure++;
        }
    }

    @Override
    public void close() {
        double successRatio = sendSuccess / (sendSuccess + sendFailure);
        log.info("发送成功率={}%", String.format("%f", successRatio * 100));
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
