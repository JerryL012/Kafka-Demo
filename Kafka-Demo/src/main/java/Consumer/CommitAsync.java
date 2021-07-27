package Consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class CommitAsync {
    private static Logger logger = LoggerFactory.getLogger(CommitAsync.class.getName());
    public static void main(String[] args) {
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
                    logger.debug("Topic: %s, Partition: %s, Offset: %s, Customer: %s, Value: %s \n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
                    consumer.commitAsync(new OffsetCommitCallback() {
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
                            if (e != null){
                                logger.error("Commit Failed For Offset {}", offsets, e);
                            }
                        }
                    });
            }
        } finally {
            consumer.close();
        }
    }
}
