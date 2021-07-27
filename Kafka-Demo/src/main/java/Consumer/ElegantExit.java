package Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class ElegantExit {

    static Logger logger = LoggerFactory.getLogger(ElegantExit.class.getName());
    static KafkaConsumer<String, String> consumer;

    public static void main(String[] args) {

        final Thread mainThread = new Thread(new coreCode());
        mainThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                logger.debug("Starting exit...");
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });
    };

    private static class coreCode implements Runnable {
        public void run() {
            Properties properties = new Properties();
            properties.put("bootstrap.servers", "kafka-01:9092, kafka-02:9092");
            properties.put("group.id", "group");
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            String topic = "topic";
            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Collections.singletonList(topic));
            try {
                while (true){
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    for (ConsumerRecord<String, String> record : records){
                        logger.debug("Topic: %s, Partition: %s, Offset: %s, Customer: %s, Value: %s \n",
                                record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    }
                    consumer.commitAsync();
                }

            } catch (WakeupException e){
                // do nothing...
            }
            finally {
                try {
                    consumer.commitSync();
                } finally {
                    consumer.close();
                }
            }
        }
    }
}
