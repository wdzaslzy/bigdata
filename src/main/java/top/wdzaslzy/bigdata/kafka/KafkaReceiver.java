package top.wdzaslzy.bigdata.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

/**
 * @author lizy
 **/
@Component
public class KafkaReceiver implements MessageListener<String, String> {
    @Override
    @KafkaListener(topics = {"calllog"})
    public void onMessage(ConsumerRecord<String, String> record) {
        System.out.println(record.topic() + " key:" + record.key() + " value:" + record.value());
    }
}
