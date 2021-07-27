package Producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;

import java.util.List;
import java.util.Map;

public class CustomPartition implements Partitioner {

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if ((keyBytes == null) || !(key instanceof String)) {
            throw  new InvalidRecordException("We expect all message have string-type key");
        }
        if (((String) key).equals("SpecificKey")){
            return numPartitions;
        }
        return 0;
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
