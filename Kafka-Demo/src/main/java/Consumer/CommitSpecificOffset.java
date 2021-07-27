package Consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

@Slf4j
public class CommitSpecificOffset {
    private HashMap<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<TopicPartition, OffsetAndMetadata>();
    int count = 0;

    public void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "kafka-01:9092, kafka-02:9092");
        properties.put("group.id", "group");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList("Topic"));
        try {
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records){
                    log.debug("Topic: %s, Partition: %s, Offset: %s, Customer: %s, Value: %s \n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    // Using Map<TopicPartition, OffsetAndMetadata>
                    currentOffset.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, "Metadata"));
                    if (count % 1000 == 0){
                        consumer.commitSync(currentOffset);
                    }
                    count++;
                }
            }
        } finally {
            consumer.close();
        }
    }
}
