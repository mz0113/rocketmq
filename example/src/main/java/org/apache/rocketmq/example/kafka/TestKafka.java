package org.apache.rocketmq.example.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.Utils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TestKafka {
    KafkaProducer kafkaProducer;
    TransactionMQProducer defaultMQProducer;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        new TestKafka().test();
    }

    private void test() throws ExecutionException, InterruptedException {
        Map map = new HashMap<>();
        map.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,MyInterceptor.class.getName());
        map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9093");
        kafkaProducer = new KafkaProducer(map);

        Future future = kafkaProducer.send(new ProducerRecord("topic", "value"));
        Object o = future.get();
    }

    class MyInterceptor implements ProducerInterceptor {

        @Override
        public ProducerRecord onSend(ProducerRecord producerRecord) {
            System.out.println("onSend");

            //发送rocketMQ，其实好像我也不用纠结kafka是否发送成功吧。
            String topic = producerRecord.topic();
            Object value = producerRecord.value();
            Object key = producerRecord.key();
            Integer partition = producerRecord.partition();
            Headers headers = producerRecord.headers();


            Message message = new Message();
            message.setBody(JSON.toJSONBytes(value));
            message.setTopic(topic);
            //key会导致hash冲突链表特别长，因此不能把key拿过来直接用，他这个key主要是做分区的选择
            if (key != null || partition != null) {
                //我们替他手动选择一个queue发送即可
            }


            for (Header header : headers) {
                String hK = header.key();
                byte[] hV = header.value();
                //TODO 这里hv字节流,能转成string吗 前端应该得拦截器去掉这个前缀吧
                message.putUserProperty("R1K2T3M_"+hK,new String(hV));
            }


            return producerRecord;
        }

        @Override
        public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
            //如果实际上发送成功了
            //如果本机宕机没收到ack调用，业务方上层肯定也挂了，毕竟我的ack调用是先于通知业务方的。
            //业务如果是异步调用不care结果，那我也可以不care结果，不提交事务，虽然kafka发出去了，但是rocketMQ会没发出去，但无所谓，因为调用方并不care这个消息
            //业务如果是同步调用，那么业务自己肯定是也挂了自己也会回滚，我也不用提交事务。
            //业务如果是异步回调，那么业务自己没有ack肯定也会回滚，我依然不需要提交事务
            //综上我只要在收到ack后再commit RocketMQ的事务即可，只要没收到ack调用一定不需要commit消息

            //如果说kafka消息压根没发成功，那我这里总之不会commit消息。
        }

        @Override
        public void close() {
            System.out.println("close");
        }

        @Override
        public void configure(Map<String, ?> map) {
            System.out.println("configure");
        }
    }

}
