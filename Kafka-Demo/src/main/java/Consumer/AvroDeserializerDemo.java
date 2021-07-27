package Consumer;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import utils.Customer;

import java.util.Collections;
import java.util.Properties;

public class AvroDeserializerDemo {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "kafka-01, kafka-02");
        properties.put("group.id", "group");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer;");
        properties.put("schema.registry.url", "schemaUrl");

        String topic = "topic";

        KafkaConsumer<String, Customer> consumer = new KafkaConsumer<String, Customer>(properties);
        consumer.subscribe(Collections.singletonList(topic));

        while (true){
            ConsumerRecords<String, Customer> records = consumer.poll(1000);
            for (ConsumerRecord<String, Customer> record : records){
                System.out.println("Current customer name: " + record.value().getCustomerName());
            }
            consumer.commitSync();
        }
    }
}
