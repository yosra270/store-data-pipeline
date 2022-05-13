package tn.insat.project;

import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.HashMap;
import java.util.Random;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import org.json.JSONObject;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

public class StoreKafkaProducer {
    static  String productNames[] = {"T-shirt","sweater", "jacket", "coat", "jeans", "socks", "shorts"};
    static Random random = new Random();
    static int maxPrice = 375;
    static int minPrice = 25;

    public static void main(String[] args) {
        if(args.length == 0){
            System.out.println("Topic name ? :");
            return;
        }
        String topicName = args[0];

        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("buffer.memory", 33554432);
        //props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //props.put("value.serializer", "org.springframework.kafka.support.serializer.JsonSerializer");


        //Thread.currentThread().setContextClassLoader(null);

        ProducerFactory<String, Purchase> producerFactory = new DefaultKafkaProducerFactory <>(props, new StringSerializer(), new JsonSerializer<Purchase>(new ObjectMapper()));
        KafkaTemplate<String, Purchase> kafkaTemplate = new KafkaTemplate<>(producerFactory);

        for(int i = 0; i < 2000; i++)
        {
            //HashMap<String, Object> record = new HashMap<>();
            //record.put("userID", generateRandomID());
            //record.put("product", generateRandomProduct());
            //record.put("price", generateRandomPrice());
            //record.put("time", generateRandomTime());
            Purchase purchase = new Purchase();
            purchase.setUserID(generateRandomID());
            purchase.setProduct(generateRandomProduct());
            purchase.setPrice(generateRandomPrice());
            purchase.setTime(generateRandomTime());

            kafkaTemplate.send(new ProducerRecord<>(topicName, Integer.toString(i), purchase));

        }
    }
    public static int generateRandomID() {
        return random.nextInt(50000)+1;
    }

    public static String generateRandomProduct() {
        return productNames[random.nextInt(productNames.length)];
    }
    public static int generateRandomPrice() {
        return (int)(Math.random()*(maxPrice-minPrice+1)+minPrice);
    }
    public static String generateRandomTime() {
        int exponent = (int) (Math.random()*5+3);
        Timestamp timestamp = new Timestamp(System.currentTimeMillis() - (long) Math.pow(10, exponent));
        String time = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(timestamp);
        return time;
    }
}