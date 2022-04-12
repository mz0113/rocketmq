/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class demonstrates how to send messages to brokers using provided {@link DefaultMQProducer}.
 */
public class Producer {
    public static void main(String[] args) throws MQClientException, InterruptedException {

        /*
         * Instantiate with a producer group name.
         */
        DefaultMQProducer producer = new DefaultMQProducer("producerGrp1");
        producer.setNamesrvAddr("127.0.0.1:9876");
/*
         * Specify name server addresses.
         * <p/>
         *
         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
         * <pre>
         * {@code
         * producer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
         * }
         * </pre>
*/

        /*
         * Launch the instance.
         */
        producer.start();

        AtomicLong atomicInteger = new AtomicLong(0);
        AtomicBoolean atomicBoolean = new AtomicBoolean(true);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (atomicInteger.get() < 80 * 10 * 1000) {
                        try {
                            /*
                             * Create a message instance, specifying topic, tag and message body.
                             */
                            Message msg = new Message("kafkaconnect" /* Topic */,
                                    "TagA" /* Tag */,
                                    ("Hello RocketMQ " + atomicInteger.incrementAndGet()).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                            );
                            /*
                             * Call send message to deliver message to one of brokers.
                             */
                            SendResult sendResult = producer.send(msg, 8000);

                            //System.out.println(String.format("%s  %s",sendResult.getQueueOffset(),new String(msg.getBody())));
                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            //producer.shutdown();
                        }
                    }
                }
            });
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        System.out.println("total count:"+atomicInteger.get());

/*        for (int i = 0; i < 10; i++) {
            try {

                *//*
                 * Create a message instance, specifying topic, tag and message body.
                 *//*
                Message msg = new Message("kafkaconnect" *//* Topic *//*,
                    "TagA" *//* Tag *//*,
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) *//* Message body *//*
                );

                *//*
                 * Call send message to deliver message to one of brokers.
                 *//*
                SendResult sendResult = producer.send(msg,100);

                System.out.printf("%s%n", sendResult);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }*/

        /*
         * Shut down once the producer instance is not longer in use.
         */
    }
}
