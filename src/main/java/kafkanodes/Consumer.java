package kafkanodes;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.data.time.Second;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import javax.swing.JFrame;
import java.awt.BorderLayout;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;



public class Consumer extends Client {

    private KafkaConsumer<String, byte[]> consumer;
    private String topic;
    private TimeSeries temperatureSeries;
    private TimeSeries humiditySeries;
    private JFrame chartFrame;

    public Consumer(String topic){
        super(topic);
        this.topic = topic;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.SERVER_NAME + ":" + Constants.PORT);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "weather-data-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));


        temperatureSeries = new TimeSeries("Temperature");
        humiditySeries = new TimeSeries("Humidity");


        TimeSeriesCollection dataset = new TimeSeriesCollection();
        dataset.addSeries(temperatureSeries);
        dataset.addSeries(humiditySeries);

        JFreeChart chart = ChartFactory.createTimeSeriesChart(
                "Weather Data Chart",
                "Time",
                "Value",
                dataset,
                true,
                true,
                false
        );

        // Create and set up the window
        chartFrame = new JFrame("Kafka Consumer Weather Data");
        chartFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        chartFrame.getContentPane().add(new ChartPanel(chart), BorderLayout.CENTER);
        chartFrame.pack();
        chartFrame.setVisible(true);

    }

    public void processMessages() {
        try {
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, byte[]> record : records) {
                    System.out.println(record.value());
                    JSONObject jsonObject = decode(record.value());
                    double temperature = jsonObject.getDouble("temperatura");
                    int humidity = jsonObject.getInt("humedad");

                    Second current = new Second();
                    temperatureSeries.addOrUpdate(current, temperature);
                    humiditySeries.addOrUpdate(current, humidity);


                    System.out.printf("Received new record: Temperature - %.2f, Humidity - %d%%\n", temperature, humidity);
                }
            }
        } finally {
            consumer.close();
        }
    }

    // Decodifica 3 bytes en un objeto JSON
    public static JSONObject decode(byte[] data) {
        int packedData = ((data[0] & 0xFF) << 16) | ((data[1] & 0xFF) << 8) | (data[2] & 0xFF);
        double temperatura = ((packedData >> 10) & 0x3FFF) / 1000.0;
        int humedad = (packedData >> 3) & 0x7F;
        String direccionViento = Constants.DIRECTIONS[packedData & 0x07];

        JSONObject json = new JSONObject();
        json.put("temperatura", temperatura);
        json.put("humedad", humedad);
        json.put("direccion_viento", direccionViento);
        return json;
    }

}

