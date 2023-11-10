package kafkanodes;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Client {
    protected static final String serverName = Constants.SERVER_NAME;
    protected static final String ip = Constants.IP;
    protected static final Integer port = Constants.PORT;
    protected Properties props;
    protected String topic;

    protected KafkaProducer<String, String> producer;

    public Client(String topic){
        this.topic = topic;
        props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ip + ":" + port.toString());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(props);
    }

    public void sendInfo(String data){
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "sensor1", data);
        producer.send(record);
        producer.flush();
    }

    public void closeProducer() {
        producer.close();
    }
}
