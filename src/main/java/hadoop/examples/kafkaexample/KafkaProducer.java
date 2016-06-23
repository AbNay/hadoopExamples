package hadoop.examples.kafkaexample;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

 public class KafkaProducer {
    private static Producer<Integer, String> producer;
    private static final String topic= "test";

    public void initialize() {
          Properties producerProps = new Properties();
          producerProps.put("metadata.broker.list", "54.183.112.17:9092");
          producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
          producerProps.put("request.required.acks", "1");
          ProducerConfig producerConfig = new ProducerConfig(producerProps);
          producer = new Producer<Integer, String>(producerConfig);
    }
    public void publishMesssage() throws Exception{            
          BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));               
      while (true){
          System.out.print("Enter message to send to kafka broker (Press 'Y' to close producer): ");
        String msg = null;
        msg = reader.readLine(); // Read message from console
        //Define topic name and message
        KeyedMessage<Integer, String> keyedMsg =
                     new KeyedMessage<Integer, String>(topic, msg);
        producer.send(keyedMsg); // This publishes message on given topic
        if("Y".equals(msg)){ break; }
        System.out.println("--> Message [" + msg + "] sent. Check message on Consumer's program console");
      }
      return;
    }

    public static void main(String[] args) throws Exception {
          KafkaProducer kafkaProducer = new KafkaProducer();
          // Initialize producer
          kafkaProducer.initialize();            
          // Publish message
          kafkaProducer.publishMesssage();
          //Close the producer
          producer.close();
    }
}
