package top.wdzaslzy.bigdata.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * @author lizy
 **/
@Component
public class KafkaSender {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void send() throws InterruptedException {
        while (true) {
            int amount = (int) (Math.random() * 999) + 1;
            int id = (int) (Math.random() * 9) + 1;

            String orderInfo = "{\"id\":" + id + ",\"amount\":" + amount + "}";
            kafkaTemplate.send("calllog", orderInfo);
            System.out.println("msg:" + orderInfo);
            Thread.sleep(1000);
        }
    }

}
