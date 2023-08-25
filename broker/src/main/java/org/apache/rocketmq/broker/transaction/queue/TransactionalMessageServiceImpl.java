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
package org.apache.rocketmq.broker.transaction.queue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.BrokerRole;

public class TransactionalMessageServiceImpl implements TransactionalMessageService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private TransactionalMessageBridge transactionalMessageBridge;

    private static final int PULL_MSG_RETRY_NUMBER = 1;

    private static final int MAX_PROCESS_TIME_LIMIT = 60000;
    private static final int MAX_RETRY_TIMES_FOR_ESCAPE = 10;

    private static final int MAX_RETRY_COUNT_WHEN_HALF_NULL = 1;

    private static final int OP_MSG_PULL_NUMS = 32;

    private static final int SLEEP_WHILE_NO_OP = 1000;

    private final ConcurrentHashMap<Integer, MessageQueueOpContext> deleteContext = new ConcurrentHashMap<>();

    private ServiceThread transactionalOpBatchService;

    private ConcurrentHashMap<MessageQueue, MessageQueue> opQueueMap = new ConcurrentHashMap<>();

    public TransactionalMessageServiceImpl(TransactionalMessageBridge transactionBridge) {
        this.transactionalMessageBridge = transactionBridge;
        transactionalOpBatchService = new TransactionalOpBatchService(transactionalMessageBridge.getBrokerController(), this);
        transactionalOpBatchService.start();
    }


    @Override
    public CompletableFuture<PutMessageResult> asyncPrepareMessage(MessageExtBrokerInner messageInner) {
        return transactionalMessageBridge.asyncPutHalfMessage(messageInner);
    }

    @Override
    public PutMessageResult prepareMessage(MessageExtBrokerInner messageInner) {
        return transactionalMessageBridge.putHalfMessage(messageInner);
    }

    private boolean needDiscard(MessageExt msgExt, int transactionCheckMax) {
        String checkTimes = msgExt.getProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES);
        int checkTime = 1;
        if (null != checkTimes) {
            checkTime = getInt(checkTimes);
            if (checkTime >= transactionCheckMax) {
                return true;
            } else {
                checkTime++;
            }
        }
        msgExt.putUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, String.valueOf(checkTime));
        return false;
    }

    private boolean needSkip(MessageExt msgExt) {
        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
        if (valueOfCurrentMinusBorn
            > transactionalMessageBridge.getBrokerController().getMessageStoreConfig().getFileReservedTime()
            * 3600L * 1000) {
            log.info("Half message exceed file reserved time ,so skip it.messageId {},bornTime {}",
                msgExt.getMsgId(), msgExt.getBornTimestamp());
            return true;
        }
        return false;
    }

    private boolean putBackHalfMsgQueue(MessageExt msgExt, long offset) {
        PutMessageResult putMessageResult = putBackToHalfQueueReturnResult(msgExt);
        if (putMessageResult != null
            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            msgExt.setQueueOffset(
                putMessageResult.getAppendMessageResult().getLogicsOffset());
            msgExt.setCommitLogOffset(
                putMessageResult.getAppendMessageResult().getWroteOffset());
            msgExt.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            log.debug(
                "Send check message, the offset={} restored in queueOffset={} "
                    + "commitLogOffset={} "
                    + "newMsgId={} realMsgId={} topic={}",
                offset, msgExt.getQueueOffset(), msgExt.getCommitLogOffset(), msgExt.getMsgId(),
                msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                msgExt.getTopic());
            return true;
        } else {
            log.error(
                "PutBackToHalfQueueReturnResult write failed, topic: {}, queueId: {}, "
                    + "msgId: {}",
                msgExt.getTopic(), msgExt.getQueueId(), msgExt.getMsgId());
            return false;
        }
    }

    @Override
    public void check(long transactionTimeout, int transactionCheckMax,
        AbstractTransactionalMessageCheckListener listener) {
        try {
            String topic = TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC;
            Set<MessageQueue> msgQueues = transactionalMessageBridge.fetchMessageQueues(topic);
            if (msgQueues == null || msgQueues.size() == 0) {
                log.warn("The queue of topic is empty :" + topic);
                return;
            }
            log.debug("Check topic={}, queues={}", topic, msgQueues);
            for (MessageQueue messageQueue : msgQueues) {
                long startTime = System.currentTimeMillis();
                MessageQueue opQueue = getOpQueue(messageQueue);
                long halfOffset = transactionalMessageBridge.fetchConsumeOffset(messageQueue);
                long opOffset = transactionalMessageBridge.fetchConsumeOffset(opQueue);
                log.info("Before check, the queue={} msgOffset={} opOffset={}", messageQueue, halfOffset, opOffset);
                if (halfOffset < 0 || opOffset < 0) {
                    log.error("MessageQueue: {} illegal offset read: {}, op offset: {},skip this queue", messageQueue,
                        halfOffset, opOffset);
                    continue;
                }

                //也就是说,这条opMessage没有关联任何half消息了,或者说关联的half消息都处理完了
                List<Long> doneOpOffset = new ArrayList<>();

                //意思就是说这条半消息对应的op消息是哪条
                HashMap<Long/*halfOffset*/, Long/*opOffsetSet*/> removeMap = new HashMap<>();

                //这个意思是就是这条op消息涉及哪些half消息
                HashMap<Long/*opOffset*/, HashSet<Long/*halfOffset*/>> opMsgMap = new HashMap<Long, HashSet<Long>>();

                //但是问题是这个opOffset一直在递增,
                PullResult pullResult = fillOpRemoveMap(removeMap, opQueue, opOffset, halfOffset, opMsgMap, doneOpOffset);
                if (null == pullResult) {
                    log.error("The queue={} check msgOffset={} with opOffset={} failed, pullResult is null",
                        messageQueue, halfOffset, opOffset);
                    continue;
                }
                // single thread
                int getMessageNullCount = 1;
                long newOffset = halfOffset;
                long i = halfOffset;
                long nextOpOffset = pullResult.getNextBeginOffset();
                int putInQueueCount = 0;
                int escapeFailCnt = 0;

                while (true) {
                    if (System.currentTimeMillis() - startTime > MAX_PROCESS_TIME_LIMIT) {
                        log.info("Queue={} process time reach max={}", messageQueue, MAX_PROCESS_TIME_LIMIT);
                        break;
                    }
                    if (removeMap/*half:op*/.containsKey(i)) { //但是这个有没有可能确实没拉到,因为只拉取32条op消息 //todo 有没有可能half对应的op在前面就被拉过了
                        log.debug("Half offset {} has been committed/rolled back", i);
                        Long removedOpOffset = removeMap.remove(i);//表示这条half消息得到处理了
                        opMsgMap/*op:half*/.get(removedOpOffset).remove(i);//取消这条op消息关联的该条half消息,因为这条half认为得到处理了
                        if (opMsgMap.get(removedOpOffset).size() == 0) { //意思就是说这条op消息关联的half消息都被处理完了
                            opMsgMap.remove(removedOpOffset);//所以不再管理这条opMsg了,从map中移除即可
                            doneOpOffset.add(removedOpOffset);//表示这条opMsg关联的half消息都处理完了
                        }
                    } else {
                        GetResult getResult = getHalfMsg(messageQueue, i);
                        MessageExt msgExt = getResult.getMsg();
                        if (msgExt == null) {
                            if (getMessageNullCount++ > MAX_RETRY_COUNT_WHEN_HALF_NULL) {
                                break;
                            }
                            if (getResult.getPullResult().getPullStatus() == PullStatus.NO_NEW_MSG) {
                                log.debug("No new msg, the miss offset={} in={}, continue check={}, pull result={}", i,
                                    messageQueue, getMessageNullCount, getResult.getPullResult());
                                break;
                            } else {
                                log.info("Illegal offset, the miss offset={} in={}, continue check={}, pull result={}",
                                    i, messageQueue, getMessageNullCount, getResult.getPullResult());
                                i = getResult.getPullResult().getNextBeginOffset();
                                newOffset = i;
                                continue;
                            }
                        }

                        if (this.transactionalMessageBridge.getBrokerController().getBrokerConfig().isEnableSlaveActingMaster()
                            && this.transactionalMessageBridge.getBrokerController().getMinBrokerIdInGroup()
                            == this.transactionalMessageBridge.getBrokerController().getBrokerIdentity().getBrokerId()
                            && BrokerRole.SLAVE.equals(this.transactionalMessageBridge.getBrokerController().getMessageStoreConfig().getBrokerRole())
                        ) {
                            final MessageExtBrokerInner msgInner = this.transactionalMessageBridge.renewHalfMessageInner(msgExt);
                            final boolean isSuccess = this.transactionalMessageBridge.escapeMessage(msgInner);

                            if (isSuccess) {
                                escapeFailCnt = 0;
                                newOffset = i + 1;
                                i++;
                            } else {
                                log.warn("Escaping transactional message failed {} times! msgId(offsetId)={}, UNIQ_KEY(transactionId)={}",
                                    escapeFailCnt + 1,
                                    msgExt.getMsgId(),
                                    msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
                                if (escapeFailCnt < MAX_RETRY_TIMES_FOR_ESCAPE) {
                                    escapeFailCnt++;
                                    Thread.sleep(100L * (2 ^ escapeFailCnt));
                                } else {
                                    escapeFailCnt = 0;
                                    newOffset = i + 1;
                                    i++;
                                }
                            }
                            continue;
                        }

                        if (needDiscard(msgExt, transactionCheckMax) || needSkip(msgExt)) {
                            listener.resolveDiscardMsg(msgExt);
                            newOffset = i + 1;
                            i++;
                            continue;
                        }
                        if (msgExt.getStoreTimestamp() >= startTime) {
                            log.debug("Fresh stored. the miss offset={}, check it later, store={}", i,
                                new Date(msgExt.getStoreTimestamp()));
                            break;
                        }

                        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
                        long checkImmunityTime = transactionTimeout;

                        //那这个逻辑基本上认为不可能走的到?因为压根不会放PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS进去啊,当然除非是用户手动指定就可以
                        String checkImmunityTimeStr = msgExt.getUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS);
                        if (null != checkImmunityTimeStr) {
                            //如果是被推迟过后的half消息
                            checkImmunityTime = getImmunityTime(checkImmunityTimeStr, transactionTimeout);
                            if (valueOfCurrentMinusBorn < checkImmunityTime) {
                                if (checkPrepareQueueOffset(removeMap, doneOpOffset, msgExt, checkImmunityTimeStr)) {
                                    newOffset = i + 1; //这条half消息太新了,还不能回查,所以要重新put到queue中,然后跳过,因此 ConsumerHalfQueueOffset+1
                                    i++;
                                    continue;
                                }
                            }
                        } else {
                            //如果没有手动指定过PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS
                            if (0 <= valueOfCurrentMinusBorn && valueOfCurrentMinusBorn < checkImmunityTime) {
                                log.debug("New arrived, the miss offset={}, check it later checkImmunity={}, born={}", i,
                                    checkImmunityTime, new Date(msgExt.getBornTimestamp()));

                                //这里直接是break,会中断循环.啥意思呢,当前这条消息是最小的offset,它都没到处理时间,后续的消息自然更没到.所以可以直接break掉
                                break;
                            }
                        }
                        //反正就是最多32条消息嘛
                        List<MessageExt> opMsg = pullResult == null ? null : pullResult.getMsgFoundList();


                        //如果opMsg为null说明这没有最新的opMsg了,此时发起回查非常合理 ,应该就是随便判断以下了,条件肯定满足,否则上面就break了不是么
                        //opMsg == null && valueOfCurrentMinusBorn > checkImmunityTime
                        boolean isNeedCheck = opMsg == null && valueOfCurrentMinusBorn > checkImmunityTime
                                /*
                                发送具体的事务回查机制，这里用一个线程池来异步发送回查消息，为了回查进度保存的简化，这里只要发送了回查消息，当前回查进度会向前推动，
                                如果回查失败，上一步骤新增的消息将可以再次发送回查消息，那如果回查消息发送成功，那会不会下一次又重复发送回查消息呢？
                                这个可以根据OP队列中的消息来判断是否重复，如果回查消息发送成功并且消息服务器完成提交或回滚操作，这条消息会发送到OP队列中,
                                然后首先会通过fillOpRemoveMap根据处理进度获取一批已处理的消息，来与消息判断是否重复，
                                由于fillopRemoveMap一次只拉32条消息，那又如何保证一定能拉取到与当前消息的处理记录呢？
                                其实就是通过代码@10，如果此批消息最后一条未超过事务延迟消息，则继续拉取更多消息进行判断（@12）和(@14),op队列也会随着回查进度的推进而推进。
                                */

                                /**
                                 * -  opMsg == null && valueOfCurrentMinusBorn > checkImmunityTime ：如果事务消息列表  opMsg  为空，并且当前消息的事务开始时间差大于事务的免检时间（checkImmunityTime），则需要进行事务回查。
                                 * -  opMsg != null && opMsg.get(opMsg.size() - 1).getBornTimestamp() - startTime > transactionTimeout ：如果事务消息列表  opMsg  不为空，并且最后一条消息的产生时间与事务开始时间的差大于事务超时时间（transactionTimeout），则需要进行事务回查。
                                 * -  valueOfCurrentMinusBorn <= -1 ：如果当前消息的事务开始时间差小于等于-1，表示事务消息的发送时间无效或异常，也需要进行事务回查。
                                 */

                                //startTime可以认为是事务开始时间,因为我们开始处理Half消息,意味着肯定事务开始了,只是有点误差,但是可以明确事务一定开始了
                                //然后拉了一次opMsg,结果拉出来最后一条消息距离事务开始时间已经过去6秒钟了
                                //考虑如果提交的非常及时的话,应该是startTime一开始,opMsg就紧随其后来了.两者几乎时间一致.所以就不会进入check
                                //但是如果提交的不及时,那么opMsg来的非常晚,像这里晚于6秒钟,就需要回查.
                                //注意我是startTime开始拉的,拉了第一批,这第一批的opMsg的bornTime最大也不会大过startTime,否则如果>startTime,比如我00秒开始拉,结果拉到了01秒落文件的消息?那显然不可能,我怎么可能拉取到未来的opMsg
                                //所以第一轮次时候,bornTime肯定小于等于startTime,所以这个条件不满足,isNeedCheck = false,会继续去fillOpRemoveMap拉opMsg
                                //在后续的轮次中,startTime不变,然后bornTime肯定是单调递增,然后一直拉一直拉,直到已经过去6秒钟了(表现就是我已经拉到了bornTime-start>transTimeout的消息)
                                //,居然还没拉到需要的opMsg(之前拉取的所有opMsg都与当前half消息尝试匹配了,只是没匹配到),此时就认为需要发起回查了.
                                //所以话说回来,这个判断就是要保证事务开始6秒内所有的opMsg都要拉取到以便进行判断.因为如果不满足6秒就会一直拉嘛,条件不满足就走else分支了.
                                || opMsg != null && opMsg.get(opMsg.size() - 1).getBornTimestamp() - startTime > transactionTimeout

                                /*
                                * 当 op 消息的集合为空，说明当前还没有收到让当前事务结束的通知，且超过了"免疫时间"，故回查
                                  当前 op 消息最大偏移量的生成时间超过了"免疫时间"，说明该事务的提交消息可能丢失了，故回查
                                  不启用 "免疫时间"
                                * */

                                //valueOfCurrentMinusBorn小于-1说明现在处理的慢了,消息是12:12秒落盘的,但是处理却是12:10秒,理论上也不太会发生吧.
                            || valueOfCurrentMinusBorn <= -1;

                        if (isNeedCheck) {
                            if (!putBackHalfMsgQueue(msgExt, i)) {
                                continue;
                            }
                            putInQueueCount++;
                            log.info("Check transaction. real_topic={},uniqKey={},offset={},commitLogOffset={}",
                                    msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC),
                                    msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                                    msgExt.getQueueOffset(), msgExt.getCommitLogOffset());
                            listener.resolveHalfMsg(msgExt);
                        } else {
                            //拉取更多的opMsg , 可也就拉32条
                            nextOpOffset = pullResult != null ? pullResult.getNextBeginOffset() : nextOpOffset;
                            pullResult = fillOpRemoveMap(removeMap, opQueue, nextOpOffset,
                                    halfOffset, opMsgMap, doneOpOffset);
                            if (pullResult == null || pullResult.getPullStatus() == PullStatus.NO_NEW_MSG
                                    || pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL
                                    || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {

                                try {
                                    Thread.sleep(SLEEP_WHILE_NO_OP);
                                } catch (Throwable ignored) {
                                }

                            } else {
                                log.info("The miss message offset:{}, pullOffsetOfOp:{}, miniOffset:{} get more opMsg.", i, nextOpOffset, halfOffset);
                            }

                            continue;
                        }
                    }
                    newOffset = i + 1;
                    i++;
                }
                if (newOffset != halfOffset) {
                    transactionalMessageBridge.updateConsumeOffset(messageQueue, newOffset);
                }
                long newOpOffset = calculateOpOffset(doneOpOffset, opOffset);
                if (newOpOffset != opOffset) {
                    transactionalMessageBridge.updateConsumeOffset(opQueue, newOpOffset);
                }
                GetResult getResult = getHalfMsg(messageQueue, newOffset);
                pullResult = pullOpMsg(opQueue, newOpOffset, 1);
                long maxMsgOffset = getResult.getPullResult() == null ? newOffset : getResult.getPullResult().getMaxOffset();
                long maxOpOffset = pullResult == null ? newOpOffset : pullResult.getMaxOffset();
                long msgTime = getResult.getMsg() == null ? System.currentTimeMillis() : getResult.getMsg().getStoreTimestamp();

                log.info("After check, {} opOffset={} opOffsetDiff={} msgOffset={} msgOffsetDiff={} msgTime={} msgTimeDelayInMs={} putInQueueCount={}",
                        messageQueue, newOpOffset, maxOpOffset - newOpOffset, newOffset, maxMsgOffset - newOffset, new Date(msgTime),
                        System.currentTimeMillis() - msgTime, putInQueueCount);
            }
        } catch (Throwable e) {
            log.error("Check error", e);
        }

    }

    private long getImmunityTime(String checkImmunityTimeStr, long transactionTimeout) {
        long checkImmunityTime;

        checkImmunityTime = getLong(checkImmunityTimeStr);
        if (-1 == checkImmunityTime) {
            checkImmunityTime = transactionTimeout;
        } else {
            checkImmunityTime *= 1000;
        }
        return checkImmunityTime;
    }

    /**
     * Read op message, parse op message, and fill removeMap
     *
     * @param removeMap Half message to be remove, key:halfOffset, value: opOffset.
     * @param opQueue Op message queue.
     * @param pullOffsetOfOp The begin offset of op message queue.
     * @param miniOffset The current minimum offset of half message queue.
     * @param opMsgMap Half message offset in op message
     * @param doneOpOffset Stored op messages that have been processed.
     * @return Op message result.
     */
    private PullResult fillOpRemoveMap(HashMap<Long, Long> removeMap, MessageQueue opQueue,
                                       long pullOffsetOfOp, long miniOffset, Map<Long, HashSet<Long>> opMsgMap, List<Long> doneOpOffset) {
        PullResult pullResult = pullOpMsg(opQueue, pullOffsetOfOp, OP_MSG_PULL_NUMS);
        if (null == pullResult) {
            return null;
        }
        if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL
            || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
            log.warn("The miss op offset={} in queue={} is illegal, pullResult={}", pullOffsetOfOp, opQueue,
                pullResult);
            transactionalMessageBridge.updateConsumeOffset(opQueue, pullResult.getNextBeginOffset());
            return pullResult;
        } else if (pullResult.getPullStatus() == PullStatus.NO_NEW_MSG) {
            log.warn("The miss op offset={} in queue={} is NO_NEW_MSG, pullResult={}", pullOffsetOfOp, opQueue,
                pullResult);
            return pullResult;
        }
        List<MessageExt> opMsg = pullResult.getMsgFoundList();
        if (opMsg == null) {
            log.warn("The miss op offset={} in queue={} is empty, pullResult={}", pullOffsetOfOp, opQueue, pullResult);
            return pullResult;
        }
        for (MessageExt opMessageExt : opMsg) {
            if (opMessageExt.getBody() == null) {
                log.error("op message body is null. queueId={}, offset={}", opMessageExt.getQueueId(),
                        opMessageExt.getQueueOffset());
                doneOpOffset.add(opMessageExt.getQueueOffset());
                continue;
            }
            HashSet<Long> set = new HashSet<Long>();
            String queueOffsetBody = new String(opMessageExt.getBody(), TransactionalMessageUtil.CHARSET);

            log.debug("Topic: {} tags: {}, OpOffset: {}, HalfOffset: {}", opMessageExt.getTopic(),
                    opMessageExt.getTags(), opMessageExt.getQueueOffset(), queueOffsetBody);
            if (TransactionalMessageUtil.REMOVE_TAG.equals(opMessageExt.getTags())) {
                String[] offsetArray = queueOffsetBody.split(TransactionalMessageUtil.OFFSET_SEPARATOR);
                for (String offset : offsetArray) {
                    Long offsetValue = getLong(offset);
                    if (offsetValue < miniOffset) {
                        continue;
                    }

                    //意思就是说这条半消息对应的op消息是哪条
                    removeMap.put(offsetValue/*halfMessageOffset*/, opMessageExt.getQueueOffset()/*opMessageOffset*/);
                    set.add(offsetValue);
                }
            } else {
                log.error("Found a illegal tag in opMessageExt= {} ", opMessageExt);
            }

            if (set.size() > 0) {
                //这个意思是就是这条op消息涉及哪些half消息
                opMsgMap.put(opMessageExt.getQueueOffset(), set);
            } else {
                //也就是说,这条opMessage没有关联任何half消息,意味着不需要进行任何操作,忽略掉就行了.
                doneOpOffset.add(opMessageExt.getQueueOffset());
            }
        }

        log.debug("Remove map: {}", removeMap);
        log.debug("Done op list: {}", doneOpOffset);
        log.debug("opMsg map: {}", opMsgMap);
        return pullResult;
    }

    /**
     * If return true, skip this msg
     *
     * @param removeMap Op message map to determine whether a half message was responded by producer.
     * @param doneOpOffset Op Message which has been checked.
     * @param msgExt Half message
     * @return Return true if put success, otherwise return false.
     */
    private boolean checkPrepareQueueOffset(HashMap<Long/*half*/, Long/*op*/> removeMap, List<Long> doneOpOffset,
        MessageExt msgExt, String checkImmunityTimeStr) {
        String prepareQueueOffsetStr = msgExt.getUserProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        if (null == prepareQueueOffsetStr) {/*注意的是这里Half消息可能也是再次经过推迟的Half消息,如果是推迟的Half消息,那么会存在PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET这个值*/
            return putImmunityMsgBackToHalfQueue(msgExt);//真正原始的half消息
        } else {
            long prepareQueueOffset = getLong(prepareQueueOffsetStr);/*原始half消息对应的offset*/
            if (-1 == prepareQueueOffset) {
                return false;
            } else {
                if (removeMap.containsKey(prepareQueueOffset)) {/*这里removeMap不含最新的opMessage啊,所以可能匹配不到*/
                    long tmpOpOffset = removeMap.remove(prepareQueueOffset);
                    doneOpOffset.add(tmpOpOffset);/*这里咋不判断opMsgMap的size了了*/
                    log.info("removeMap contain prepareQueueOffset. real_topic={},uniqKey={},immunityTime={},offset={}",
                            msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC),
                            msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                            checkImmunityTimeStr,
                            msgExt.getQueueOffset());
                    return true;
                } else {
                    return putImmunityMsgBackToHalfQueue(msgExt);
                }
            }
        }
    }

    /**
     * Write messageExt to Half topic again
     *
     * @param messageExt Message will be write back to queue
     * @return Put result can used to determine the specific results of storage.
     */
    private PutMessageResult putBackToHalfQueueReturnResult(MessageExt messageExt) {
        PutMessageResult putMessageResult = null;
        try {
            MessageExtBrokerInner msgInner = transactionalMessageBridge.renewHalfMessageInner(messageExt);
            putMessageResult = transactionalMessageBridge.putMessageReturnResult(msgInner);
        } catch (Exception e) {
            log.warn("PutBackToHalfQueueReturnResult error", e);
        }
        return putMessageResult;
    }

    private boolean putImmunityMsgBackToHalfQueue(MessageExt messageExt) {
        MessageExtBrokerInner msgInner = transactionalMessageBridge.renewImmunityHalfMessageInner(messageExt);
        return transactionalMessageBridge.putMessage(msgInner);
    }

    /**
     * Read half message from Half Topic
     *
     * @param mq Target message queue, in this method, it means the half message queue.
     * @param offset Offset in the message queue.
     * @param nums Pull message number.
     * @return Messages pulled from half message queue.
     */
    private PullResult pullHalfMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getHalfMessage(mq.getQueueId(), offset, nums);
    }

    /**
     * Read op message from Op Topic
     *
     * @param mq Target Message Queue
     * @param offset Offset in the message queue
     * @param nums Pull message number
     * @return Messages pulled from operate message queue.
     */
    private PullResult pullOpMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getOpMessage(mq.getQueueId(), offset, nums);
    }

    private Long getLong(String s) {
        long v = -1;
        try {
            v = Long.parseLong(s);
        } catch (Exception e) {
            log.error("GetLong error", e);
        }
        return v;

    }

    private Integer getInt(String s) {
        int v = -1;
        try {
            v = Integer.parseInt(s);
        } catch (Exception e) {
            log.error("GetInt error", e);
        }
        return v;

    }

    private long calculateOpOffset(List<Long> doneOffset, long oldOffset) {
        Collections.sort(doneOffset);
        long newOffset = oldOffset;
        for (int i = 0; i < doneOffset.size(); i++) {
            if (doneOffset.get(i) == newOffset) {
                newOffset++;
            } else {
                break;
            }
        }
        return newOffset;

    }

    private MessageQueue getOpQueue(MessageQueue messageQueue) {
        MessageQueue opQueue = opQueueMap.get(messageQueue);
        if (opQueue == null) {
            opQueue = new MessageQueue(TransactionalMessageUtil.buildOpTopic(), messageQueue.getBrokerName(),
                messageQueue.getQueueId());
            opQueueMap.put(messageQueue, opQueue);
        }
        return opQueue;

    }

    private GetResult getHalfMsg(MessageQueue messageQueue, long offset) {
        GetResult getResult = new GetResult();

        PullResult result = pullHalfMsg(messageQueue, offset, PULL_MSG_RETRY_NUMBER);
        if (result != null) {
            getResult.setPullResult(result);
            List<MessageExt> messageExts = result.getMsgFoundList();
            if (messageExts == null || messageExts.size() == 0) {
                return getResult;
            }
            getResult.setMsg(messageExts.get(0));
        }
        return getResult;
    }

    private OperationResult getHalfMessageByOffset(long commitLogOffset) {
        OperationResult response = new OperationResult();
        MessageExt messageExt = this.transactionalMessageBridge.lookMessageByOffset(commitLogOffset);
        if (messageExt != null) {
            response.setPrepareMessage(messageExt);
            response.setResponseCode(ResponseCode.SUCCESS);
        } else {
            response.setResponseCode(ResponseCode.SYSTEM_ERROR);
            response.setResponseRemark("Find prepared transaction message failed");
        }
        return response;
    }

    @Override
    public boolean deletePrepareMessage(MessageExt messageExt) {
        Integer queueId = messageExt.getQueueId();
        MessageQueueOpContext mqContext = deleteContext.get(queueId);
        if (mqContext == null) {
            mqContext = new MessageQueueOpContext(System.currentTimeMillis(), 20000);
            MessageQueueOpContext old = deleteContext.putIfAbsent(queueId, mqContext);
            if (old != null) {
                mqContext = old;
            }
        }

        String data = messageExt.getQueueOffset() + TransactionalMessageUtil.OFFSET_SEPARATOR;
        try {
            boolean res = mqContext.getContextQueue().offer(data, 100, TimeUnit.MILLISECONDS);
            if (res) {
                int totalSize = mqContext.getTotalSize().addAndGet(data.length());
                if (totalSize > transactionalMessageBridge.getBrokerController().getBrokerConfig().getTransactionOpMsgMaxSize()) {
                    this.transactionalOpBatchService.wakeup();
                }
                //如果添加成功了,就等后续批量落文件
                return true;
            } else {
                this.transactionalOpBatchService.wakeup();
            }
        } catch (InterruptedException ignore) {
        }

        //否则强制让OpMessage落文件
        Message msg = getOpMessage(queueId, data);
        if (this.transactionalMessageBridge.writeOp(queueId, msg)) {
            log.warn("Force add remove op data. queueId={}", queueId);
            return true;
        } else {
            log.error("Transaction op message write failed. messageId is {}, queueId is {}", messageExt.getMsgId(), messageExt.getQueueId());
            return false;
        }
    }

    @Override
    public OperationResult commitMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public OperationResult rollbackMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public boolean open() {
        return true;
    }

    @Override
    public void close() {

    }

    public Message getOpMessage(int queueId, String moreData) {
        String opTopic = TransactionalMessageUtil.buildOpTopic();
        MessageQueueOpContext mqContext = deleteContext.get(queueId);

        int moreDataLength = moreData != null ? moreData.length() : 0;
        int length = moreDataLength;
        int maxSize = transactionalMessageBridge.getBrokerController().getBrokerConfig().getTransactionOpMsgMaxSize();
        if (length < maxSize) {
            int sz = mqContext.getTotalSize().get();
            if (sz > maxSize || length + sz > maxSize) {
                length = maxSize + 100;
            } else {
                length += sz;
            }
        }

        StringBuilder sb = new StringBuilder(length);

        if (moreData != null) {
            sb.append(moreData);
        }

        while (!mqContext.getContextQueue().isEmpty()) {
            if (sb.length() >= maxSize) {
                break;
            }
            String data = mqContext.getContextQueue().poll();
            if (data != null) {
                sb.append(data);
            }
        }

        if (sb.length() == 0) {
            return null;
        }

        int l = sb.length() - moreDataLength;
        mqContext.getTotalSize().addAndGet(-l);
        mqContext.setLastWriteTimestamp(System.currentTimeMillis());
        return new Message(opTopic, TransactionalMessageUtil.REMOVE_TAG,
                sb.toString().getBytes(TransactionalMessageUtil.CHARSET));
    }
    public long batchSendOpMessage() {
        long startTime = System.currentTimeMillis();
        try {
            long firstTimestamp = startTime;
            Map<Integer, Message> sendMap = null;
            long interval = transactionalMessageBridge.getBrokerController().getBrokerConfig().getTransactionOpBatchInterval();
            int maxSize = transactionalMessageBridge.getBrokerController().getBrokerConfig().getTransactionOpMsgMaxSize();
            boolean overSize = false;
            for (Map.Entry<Integer, MessageQueueOpContext> entry : deleteContext.entrySet()) {
                MessageQueueOpContext mqContext = entry.getValue();
                //no msg in contextQueue
                if (mqContext.getTotalSize().get() <= 0 || mqContext.getContextQueue().size() == 0 ||
                        // wait for the interval
                        mqContext.getTotalSize().get() < maxSize &&
                                startTime - mqContext.getLastWriteTimestamp() < interval) {
                    continue;
                }

                if (sendMap == null) {
                    sendMap = new HashMap<>();
                }

                Message opMsg = getOpMessage(entry.getKey(), null);
                if (opMsg == null) {
                    continue;
                }
                sendMap.put(entry.getKey(), opMsg);
                firstTimestamp = Math.min(firstTimestamp, mqContext.getLastWriteTimestamp());
                if (mqContext.getTotalSize().get() >= maxSize) {
                    overSize = true;
                }
            }

            if (sendMap != null) {
                for (Map.Entry<Integer, Message> entry : sendMap.entrySet()) {
                    if (!this.transactionalMessageBridge.writeOp(entry.getKey(), entry.getValue())) {
                        log.error("Transaction batch op message write failed. body is {}, queueId is {}",
                                new String(entry.getValue().getBody(), TransactionalMessageUtil.CHARSET), entry.getKey());
                    }
                }
            }

            log.debug("Send op message queueIds={}", sendMap == null ? null : sendMap.keySet());

            //wait for next batch remove
            long wakeupTimestamp = firstTimestamp + interval;
            if (!overSize && wakeupTimestamp > startTime) {
                return wakeupTimestamp;
            }
        } catch (Throwable t) {
            log.error("batchSendOp error.", t);
        }

        return 0L;
    }

    public Map<Integer, MessageQueueOpContext> getDeleteContext() {
        return this.deleteContext;
    }
}
