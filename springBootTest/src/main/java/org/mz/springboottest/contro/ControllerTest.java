package org.mz.springboottest.contro;

import com.bestpay.devops.mq.MQSendAPI;
import com.bestpay.devops.mq.MQSendCallBack;
import com.bestpay.devops.mq.MQSendResult;
import com.bestpay.devops.mq.MQType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Component
@RequestMapping("/test")
public class ControllerTest {
    @Autowired
    MQSendAPI mqSendAPI;

    @RequestMapping("/send")
    public String send(){
        final MQSendResult result = mqSendAPI.send("middleware", MessageBuilder.withPayload("hello").setHeader(MQSendAPI.RocketMQHeaders.DELAY,2).build(), MQType.ROCKET);
/*        mqSendAPI.send("middleware", MessageBuilder.withPayload("hello").setHeader(MQSendAPI.RocketMQHeaders.DELAY, 2).build(),
                new MQSendCallBack() {
                    @Override
                    public void onSuccess(MQSendResult sendResult) {
                        System.out.println("do ok");
                    }
                    @Override
                    public void onException(Throwable throwable) {
                        System.out.println("do failed");
                    }
                },MQType.ROCKET);

        mqSendAPI.send("middleware",mqSendAPI.wrap("hello","orderId",2),MQType.ROCKET);

        mqSendAPI.sendOneway("middleware", MessageBuilder.withPayload("hello").setHeader(MQSendAPI.RocketMQHeaders.DELAY,2).build(),MQType.ROCKET);
        mqSendAPI.send("middleware", MessageBuilder.withPayload("hello").setHeader(MQSendAPI.RocketMQHeaders.DELAY,2).build(),200,MQType.ROCKET);
        mqSendAPI.send("middleware", MessageBuilder.withPayload("hello").setHeader(MQSendAPI.RocketMQHeaders.DELAY, 2).build(),
                new MQSendCallBack() {
                    @Override
                    public void onSuccess(MQSendResult sendResult) {
                        System.out.println("do ok");
                    }

                    @Override
                    public void onException(Throwable throwable) {
                        System.out.println("do failed");
                    }
                },200,MQType.ROCKET);*/
        try {
            result.getObj();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result.isOK()?"success":"failed";
    }
}
