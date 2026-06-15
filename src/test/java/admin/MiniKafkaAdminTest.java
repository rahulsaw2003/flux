package admin;

import org.junit.jupiter.api.Test;
import producer.MiniKafkaProducer;
import producer.ProducerRecord;

import java.util.List;
import java.util.Properties;

public class MiniKafkaAdminTest {

    @Test
    public void createBootstrapClusterTest() {
        Properties adminProps = new Properties();
        adminProps.setProperty("bootstrap.servers", "localhost:50051,localhost:50052,localhost:50053");
        Admin admin = MiniKafkaAdminClient.create(adminProps);

        NewTopic topic = new NewTopic("test-topic", 3, 1);
        admin.createTopics(List.of(topic));

        Properties producerProps = new Properties();

        MiniKafkaProducer<String, String> producer = new MiniKafkaProducer<>(producerProps, 15, 60);
        while (true) {
            ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", 2, "test-key", "test-value");
            try {
                producer.send(record);
                Thread.sleep(1000);
            } catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
