package org.apache.rocketmq.example.offset;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.OffsetWrapper;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Consumer1 {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumerG1");
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
               // int a = 3 / 0;
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.setPersistConsumerOffsetInterval(1000 * 60 * 10);
        consumer.setPullInterval(2000);

/*        consumer.setConsumerGroup("consumerG1Order");
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

        final MQClientInstance mqClientInstance = consumer.getDefaultMQPushConsumerImpl().getmQClientFactory();
        try {
            final ConsumeStats consumeStats = mqClientInstance.getMQClientAPIImpl().getConsumeStats("127.0.0.1:10911", "consumerG2", 5000);
            System.out.println(JSON.toJSONString(consumeStats, SerializerFeature.PrettyFormat));
            final HashMap<MessageQueue, OffsetWrapper> offsetTable = consumeStats.getOffsetTable();
            for (Map.Entry<MessageQueue, OffsetWrapper> entry : offsetTable.entrySet()) {
                final MessageQueue key = entry.getKey();
                if (key.getTopic().equals("offsetTest")) {
                    final OffsetWrapper wrapper = entry.getValue();
                    final long consumerOffset = wrapper.getConsumerOffset();
                    consumer.getDefaultMQPushConsumerImpl().updateConsumeOffset(key,consumerOffset);

                    consumer.getDefaultMQPushConsumerImpl().persistConsumerOffset();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
