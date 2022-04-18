package org.mz.springboottest.contro;

import com.bestpay.devops.rocketmq.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

@RocketMQTransactionListener(rocketMQSendAPIBeanName = "rocketMQSendAPIImpl")
@Component
public class TransListener implements TransactionListener {
    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        return null;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        return null;
    }
}
