package kafkanodes;

import java.util.concurrent.ThreadLocalRandom;

public class Producer extends Client {

    public Producer(String topic){
        super(topic);
    }

    public String generateTemperature() {
        double mean = 50.0;
        double variance = 10.0;
        double temperature = ThreadLocalRandom.current().nextGaussian() * Math.sqrt(variance) + mean;
        temperature = Math.max(0, Math.min(100, temperature));
        return String.format("%.2f", temperature);
    }

    public int generateHumidity() {
        double mean = 50.0;
        double variance = 10.0;
        int humidity = (int) (ThreadLocalRandom.current().nextGaussian() * Math.sqrt(variance) + mean);
        humidity = Math.max(0, Math.min(100, humidity));
        return humidity;
    }

    public String generateWindDirection() {
        String[] directions = {"N", "NW", "W", "SW", "S", "SE", "E", "NE"};
        int index = ThreadLocalRandom.current().nextInt(directions.length);
        return directions[index];
    }

    public String generateWeatherData() {
        String temperature = generateTemperature();
        int humidity = generateHumidity();
        String windDirection = generateWindDirection();
        return String.format("{\"temperatura\":%s, \"humedad\":%d, \"direccion_viento\":\"%s\"}", temperature, humidity, windDirection);
    }}
