package kafkanodes;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Client {
    protected static final String serverName = Constants.SERVER_NAME;
    protected static final String ip = Constants.IP;
    protected static final Integer port = Constants.PORT;
    protected Properties props;
    protected String topic;

    protected KafkaProducer<String, byte[]> producer;

    public Client(String topic){
        this.topic = topic;
        props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ip + ":" + port.toString());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        producer = new KafkaProducer<String, byte[]>(props);
    }

    public void sendInfo(byte[] data){
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, "sensor1", data);
        producer.send(record);
        producer.flush();
    }

    public void closeProducer() {
        producer.close();
    }
}
