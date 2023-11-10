package kafkanodes;

public class MainConsumer {

    public static void main(String[] args) {
        String topic = "20008";
        Consumer consumer = new Consumer(topic);
        consumer.processMessages();
    }
}
