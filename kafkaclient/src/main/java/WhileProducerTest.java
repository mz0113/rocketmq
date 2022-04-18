import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class WhileProducerTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        List<Header> headerList = new ArrayList<>();
        headerList.add(new RecordHeader("head-1","this is head-1 body".getBytes(StandardCharsets.UTF_8)));
        headerList.add(new RecordHeader("head-2","this is head-2 body".getBytes(StandardCharsets.UTF_8)));
        int i = 30*10000;
        while (true){
            i++;
            producer.send(new ProducerRecord("kafkaconnect",null, (Object) null, String.format("This is Body-%s",i),headerList)).get();

            if(i==10000*100){
                break;
            }
        }
        System.out.println("send OK");
    }
}
