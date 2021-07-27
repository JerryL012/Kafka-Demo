package Producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import utils.Customer;

import java.util.Properties;
import java.util.Random;

public class AvroSerializerDemo {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        properties.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        properties.put("schema.registry.url","schemaUrl");

        String topic = "Topic";

        KafkaProducer<String, Customer> producer = new KafkaProducer<String, Customer>(properties);
        Random random = new Random();
        while (true){
            Customer customer = new Customer(random.nextInt(10), "");
            System.out.println("Generated customer" + customer);
            ProducerRecord<String, Customer> record = new ProducerRecord<String, Customer>(topic, customer);
            producer.send(record);
        }
    }
}