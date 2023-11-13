package kafkanodes;

import org.json.JSONObject;

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
        int index = ThreadLocalRandom.current().nextInt(Constants.DIRECTIONS.length);
        return Constants.DIRECTIONS[index];
    }

    public String generateWeatherData() {
        String temperature = generateTemperature();
        int humidity = generateHumidity();
        String windDirection = generateWindDirection();
        return String.format("{\"temperatura\":%s, \"humedad\":%d, \"direccion_viento\":\"%s\"}", temperature, humidity, windDirection);
    }

    // Codifica un objeto JSON en 3 bytes
    public static byte[] encode(JSONObject json) {
        int temperature = (int) (json.getDouble("temperatura") * 1000);
        int humidity = json.getInt("humedad");
        int windDirection = java.util.Arrays.asList(Constants.DIRECTIONS).indexOf(json.getString("direccion_viento"));

        int packedData = temperature << 10 | humidity << 3 | windDirection;
        byte[] encoded = new byte[3];
        encoded[0] = (byte) (packedData >> 16);
        encoded[1] = (byte) (packedData >> 8);
        encoded[2] = (byte) packedData;
        return encoded;
    }

}
