package shared;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class KafkaResponsePartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String k = new String(keyBytes, StandardCharsets.UTF_8);
        String p = k.split("-")[0];
        int numPartitions = cluster.partitionsForTopic(topic).size();
        try {
            int partition = Integer.parseInt(p.trim());
            return partition % numPartitions;
        } catch (NumberFormatException nfe) {
            return 0;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
