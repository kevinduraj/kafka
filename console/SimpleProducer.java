/* ----------------------------------------------------------------------------------------------------------
 * Download: http://apache.mirrors.pair.com/kafka/0.8.2.1/kafka_2.9.1-0.8.2.1.tgz
 *
 * Start Zookeeper:
 * bin/zookeeper-server-start.sh config/zookeeper.properties
 * 
 * Start Kafka Broker:
 * bin/kafka-server-start.sh config/server.properties
 *
 * Create a Topic:
 * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
 *
 * List of topics:
 * bin/kafka-topics.sh --list --zookeeper localhost:2181
 *
 * Start producer:
 * bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
 * 
 * Start Consumer:
 * bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning
 *
 * ----------------------------------------------------------------------------------------------------------
 *
 * .bashrc
 * export KAFKA_LIB='/opt/kafka_2.9.1-0.8.2.1/libs'
 * export CLASSPATH=.:$KAFKA_LIB/jopt-simple-3.2.jar:$KAFKA_LIB/kafka-clients-0.8.2.1.jar
 *                   :$KAFKA_LIB/kafka_2.9.1-0.8.2.1.jar:$KAFKA_LIB/log4j-1.2.16.jar
 *                   :$KAFKA_LIB/metrics-core-2.2.0.jar:$KAFKA_LIB/scala-library-2.9.1.jar
 *                   :$KAFKA_LIB/slf4j-api-1.7.6.jar:$KAFKA_LIB/slf4j-log4j12-1.6.1.jar
 *                   :$KAFKA_LIB/snappy-java-1.1.1.6.jar:$KAFKA_LIB/zkclient-0.3.jar
 *                   :$KAFKA_LIB/zookeeper-3.4.6.jar
 * 
 * Check for missing class:
 * jar tvf /opt/kafka_2.9.1-0.8.2.1/libs/kafka-clients-0.8.2.1.jar | grep "org/apache/kafka/common/utils/Utils"
 *
 * Compile : javac SimpleProducer.java 
 * Run     : java SimpleProducer test 3
 *
 -------------------------------------------------------------------------------------------------------------*/

import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class SimpleProducer {

    private static Producer producer;

    public SimpleProducer() {

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

        String topic = args[0];
        String count = args[1];
        
        int messageCount = Integer.parseInt(count);
        System.out.println(" Topic Name - " + topic);
        System.out.println(" Message Count - " + messageCount);

        SimpleProducer simpleProducer = new SimpleProducer();
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

