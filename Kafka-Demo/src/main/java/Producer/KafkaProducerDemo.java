package Producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class KafkaProducerDemo {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "kafka-broker");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer(kafkaProps);

        ProducerRecord<String, String> record = new ProducerRecord<String, String>("Topic", "Key", "Value");

        // Three Way To Send Data

        // First: Fire and forget
        try {
            producer.send(record);
        } catch (Exception e){
            e.printStackTrace();
        }

        // Second: Synchronous Send
        try {
            producer.send(record).get();
        } catch (Exception e){
            e.printStackTrace();
        }

        // Third: Asynchronous Send
        producer.send(record, new DemoProducerCallback());
    }
    private static class DemoProducerCallback implements Callback {

        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null){
                e.printStackTrace();
            }
        }
    }
}
