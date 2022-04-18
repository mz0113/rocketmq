import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;

public class KafkaConsumerWhileTest {
    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList("kafkaconnect"));
        Set<Integer> integerSet = new HashSet<>();


        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true){
                    ConsumerRecords<String, String> records = consumer.poll(1000);
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
                        final String body = record.value();
                        final Integer value = Integer.valueOf(body.split("-")[1]);
                        synchronized (integerSet) {
                            if (integerSet.contains(value)) {
                                System.out.println("failed!!!!!!!!!!!!!!!!");
                            }else{
                                integerSet.add(value);
                            }
                        }
                    }
                }
            }
        }).start();


        try {
            Thread.sleep(20*1000);
            System.out.println("totalSize:"+integerSet.size());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
