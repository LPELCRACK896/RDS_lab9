package kafkanodes;

import org.json.JSONObject;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

public class MainProducer {
    public static void loopProduce(Producer pd){
        try (Scanner scanner = new Scanner(System.in)) {
            boolean run = true;
            while (run) {
                String data = pd.generateWeatherData();
                JSONObject jsonObject = new JSONObject(data);
                System.out.println(data);
                byte[] encodedData = pd.encode(jsonObject);
                System.out.println(encodedData);
                pd.sendInfo(encodedData);
                int sleepTime = ThreadLocalRandom.current().nextInt(15, 31);
                Thread.sleep(sleepTime * 1000L);

                System.out.println("Presiona Enter para detener...");
                if (System.in.available() > 0) {
                    run = false;
                    scanner.nextLine();
                }
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        } finally {
            pd.closeProducer();
        }
    }
    public static void main(String[] args) {
        String topic = "20008";
        Producer pd = new Producer(topic);
        loopProduce(pd);
    }
}
