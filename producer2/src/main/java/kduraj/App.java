package kduraj;

import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class App {

    private static Producer producer;

    public App() {

        Properties props = new Properties();
        props.put("metadata.broker.list", "127.0.0.1:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer(config);
    }

    public static void main(String[] args) {

        System.out.println("Simple Producer Main");

        int argsCount = args.length;

        if (argsCount == 0 || argsCount == 1)
            throw new IllegalArgumentException("Please provide topic name and Message count as arguments");

        String topic = (String) args[0];
        String count = (String) args[1];

        int messageCount = Integer.parseInt(count);
        System.out.println(" Topic Name - " + topic);
        System.out.println(" Message Count - " + messageCount);

        App simpleProducer = new App();
        simpleProducer.publishMessage(topic, messageCount);

    }

    private void publishMessage(String topic, int messageCount) {

        for (int mCount = 0; mCount < messageCount; mCount ++) {

            String runtime = new Date(). toString();
            String msg = "Message Publishing Time - " + runtime; System.out.println(msg);
            KeyedMessage < String, String > data = new KeyedMessage < String, String >(topic, msg);
            producer.send(data);
        }
        producer.close();
    }
}
