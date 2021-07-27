package Consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

public class RebalanceListener {
    Logger logger = LoggerFactory.getLogger(RebalanceListener.class.getName());
    private HashMap<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<TopicPartition, OffsetAndMetadata>();
    KafkaConsumer<String, String> consumer = null;

    public void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "kafka-01:9092, kafka-02:9092");
        properties.put("group.id", "group");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        String topic = "topic";
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        try {
            consumer.subscribe(Collections.singletonList(topic), new HandleRebalance());

//            consumer.poll(0); // Let the consumer join the consumer group.
//            for (TopicPartition partition : consumer.assignment()){
//                consumer.seek(partition, offset);
//            }

            while (true){
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record: records){
                    logger.debug("Topic: %s, Partition: %s, Offset: %s, Customer: %s, Value: %s \n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    currentOffset.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, "Metadata"));
                }
                consumer.commitAsync(currentOffset, null);
            }
        } catch (WakeupException e){

        } catch (Exception e){
            logger.error("Unexpected Error.", e);
        } finally {
            try {
                consumer.commitSync(currentOffset);
            } finally {
              consumer.close();
            }
        }
    }

    private class HandleRebalance implements ConsumerRebalanceListener {

        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.debug("Committing Current Offset:", currentOffset);
            consumer.commitSync(currentOffset);
        }

        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
           // consumer.seek(TopicPartition partition, offset);
        }
    }
}
