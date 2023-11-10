package kafkanodes;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.TopicPartition;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;



public class Consumer extends Client {

    private KafkaConsumer<String, String> consumer;
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
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
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

    public void getLatestRecord() {
        TopicPartition topicPartition = new TopicPartition(this.topic, 0);

        // Manually assign the partition to the consumer
        consumer.assign(Collections.singletonList(topicPartition));

        // Make sure to poll once to get the partition assignment initialized
        consumer.poll(Duration.ofMillis(0));

        // Now it's safe to seek to the end
        consumer.seekToEnd(Collections.singletonList(topicPartition));

        // Since seekToEnd is lazy, we need to poll again to get the actual last offset
        consumer.poll(Duration.ofMillis(0));

        // Get the last offset of the partition
        long lastOffset = consumer.position(topicPartition);

        // If there is no record, lastOffset will be 0 and we don't need to seek
        if (lastOffset > 0) {
            consumer.seek(topicPartition, lastOffset - 1);
        }

        // Now we poll for the actual data
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

        // It's possible that there are no records if the last message was deleted due to retention policy
        if (!records.isEmpty()) {
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Consumed record with key %s and value %s\n", record.key(), record.value());
                // Parse the JSON record as needed
                JSONObject jsonObject = new JSONObject(record.value());
                System.out.println("Temperatura: " + jsonObject.getDouble("temperatura"));
                System.out.println("Humedad: " + jsonObject.getInt("humedad"));
                System.out.println("Dirección del viento: " + jsonObject.getString("direccion_viento"));
            }
        } else {
            System.out.println("No records found at the last offset.");
        }

        consumer.close();
    }

    public void processMessages() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    JSONObject jsonObject = new JSONObject(record.value());
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
}

