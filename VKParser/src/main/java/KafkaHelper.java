import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaHelper {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaHelper.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static KafkaProducer<String, byte[]> createProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "1");
        props.put("linger.ms", "50");
        props.put("compression.type", "gzip");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static void sendJson(KafkaProducer<String, byte[]> producer, String topic, String key, Map<String, Object> value) {
        try {
            byte[] payload = MAPPER.writeValueAsBytes(value);
            producer.send(new ProducerRecord<>(topic, key, payload));
        } catch (Exception e) {
            LOG.error("Failed to send to Kafka", e);
        }
    }

        public static void sendJson(
                                    KafkaProducer<String, byte[]> producer,
                                    String topic,
                                    Map<String, Object> value
                                    ) {
        try {
            byte[] payload = MAPPER.writeValueAsBytes(value);
            producer.send(new ProducerRecord<>(topic, payload));
        } catch (Exception e) {
            LOG.error("Failed to send to Kafka", e);
        }
    }

        public static void sendJsonToPartition(
            KafkaProducer<String, byte[]> producer,
            String topic,
            int groupId,
            String key,
            Map<String, Object> value
        ) {
        try {
            byte[] payload = MAPPER.writeValueAsBytes(value);
            
            // Определяем партицию на основе ID группы
            int partition = calculatePartition(groupId);
            
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                topic, partition, key, payload
            );
            
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOG.error("Failed to send to Kafka partition {}: {}", partition, exception.getMessage());
                } else {
                    LOG.debug("Sent to topic {}, partition {}, group {}", topic, partition, groupId);
                }
            });
            
        } catch (Exception e) {
            LOG.error("Failed to send to Kafka with partitioning", e);
        }
    }
        private static int calculatePartition(int groupId) {
        // Берем абсолютное значение (т.к. группы имеют отрицательные ID в VK API)
        int absGroupId = Math.abs(groupId);
        return absGroupId;
    }

        public static void sendJsonToPartition(
                                                KafkaProducer<String, byte[]> producer,
                                                String topic,
                                                int groupId,
                                                Map<String, Object> value
                                                ) {
        try {
            byte[] payload = MAPPER.writeValueAsBytes(value);
            
            // Определяем партицию на основе ID группы
            int partition = calculatePartition(groupId);
            
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                topic, partition, null, payload
            );
            
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOG.error("Failed to send to Kafka partition {}: {}", partition, exception.getMessage());
                }
            });
            
        } catch (Exception e) {
            LOG.error("Failed to send to Kafka with partitioning", e);
        }
    }
}