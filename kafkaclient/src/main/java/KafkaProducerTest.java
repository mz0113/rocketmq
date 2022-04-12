import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerTest {

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
        producer.send(new ProducerRecord("kafkaconnect", null,"order-2","This is Body",headerList)).get();
        System.out.println("send OK");
    }
}
