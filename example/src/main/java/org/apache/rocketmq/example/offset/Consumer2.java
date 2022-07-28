package org.apache.rocketmq.example.offset;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class Consumer2 {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumerG2");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.subscribe("offsetTest","*");
        consumer.setConsumeMessageBatchMaxSize(1);
        consumer.setMaxReconsumeTimes(3);
        consumer.setMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                final MessageExt messageExt = msgs.get(0);
                final String msgId = messageExt.getMsgId();
                System.out.printf("receive msgId:%s,keys:%s \n",msgId, messageExt.getKeys());
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

/*        consumer.setConsumerGroup("consumerG2Order");
        consumer.setMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                final MessageExt messageExt = msgs.get(0);
                final String msgId = messageExt.getMsgId();
                System.out.printf("receive msgId:%s,keys:%s \n",msgId, messageExt.getKeys());
                int a = 3/0;
                return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
            }
        });*/
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.start();
    }
}
