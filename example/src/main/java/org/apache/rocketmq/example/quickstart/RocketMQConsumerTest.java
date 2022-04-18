package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 单元测试
 */
public class RocketMQConsumerTest {
    public static void main(String[] args) throws InterruptedException, MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("kafkaconnect");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe("kafkaconnect", "*");
        consumer.setConsumerGroup(UUID.randomUUID().toString());
        /**
         * 位移提交的说明
         * 如果是集群消费-并发消费的话，msgs的size可能>1,如果maxSpan过大会导致无法继续拉取。另外
         */
        AtomicInteger last = new AtomicInteger(0);
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg : msgs) {
                    final String body = new String(msg.getBody());
                    System.out.println(System.currentTimeMillis()+" Receive New Messages: "+ body +"    keys:"+msg.getKeys() + "    header:"+msg.getUserProperty("head-1") + "    header:"+msg.getUserProperty("head-2"));
                    if (last.get() + 1 == Integer.valueOf(body.split("-")[1])) {
                        last.incrementAndGet();
                    }else{
                        System.out.println("failed!!!!!!!!!!!!!!!!");
                        //consumer.shutdown();
                    }
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        /*
         *  Launch the consumer instance.
         */
        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
