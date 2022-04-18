package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 单元测试
 */
public class RocketMQConsumerConcurrentTest {
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
        Set<Integer> integerSet = new HashSet<>();
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg : msgs) {
                    final String body = new String(msg.getBody());
                    System.out.println(System.currentTimeMillis()+" Receive New Messages: "+ body +"    keys:"+msg.getKeys() + "    header:"+msg.getUserProperty("head-1") + "    header:"+msg.getUserProperty("head-2"));
                    final Integer value = Integer.valueOf(body.split("-")[1]);
                    synchronized (integerSet) {
                        if (integerSet.contains(value)) {
                            System.out.println("failed!!!!!!!!!!!!!!!!");
                        }else{
                            integerSet.add(value);
                        }
                    }
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        /*
         *  Launch the consumer instance.
         */
        consumer.start();


        Thread.sleep(20*1000);
        System.out.println("totalSize:"+integerSet.size());

        System.out.printf("Consumer Started.%n");
    }
}
