package org.mz.springboottest;

import com.bestpay.devops.mq.MQSendAPI;
import com.bestpay.devops.mq.MQSendAPIImpl;
import com.bestpay.devops.rocketmq.support.RocketMQSendAPIImpl;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class SpringBootTestApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringBootTestApplication.class, args);
    }

    @Bean("mqSendAPI")
    MQSendAPI mqSendAPI(){
        return new MQSendAPIImpl();
    }

    @Bean("rocketMQSendAPIImpl")
    RocketMQSendAPIImpl rocketMQSendAPIImpl(){
        final RocketMQSendAPIImpl rocketMQSendAPI = new RocketMQSendAPIImpl();

        rocketMQSendAPI.setNameSrv("127.0.0.1:9876");
        rocketMQSendAPI.setProducerGroup("middleware-group");

        rocketMQSendAPI.setEnv("PROD");
        rocketMQSendAPI.setIdc("NJ1");
        rocketMQSendAPI.setCity("NJ");
        rocketMQSendAPI.setZone("RZ001");
        rocketMQSendAPI.setGbgroup("A");

        return rocketMQSendAPI;
    }

}
