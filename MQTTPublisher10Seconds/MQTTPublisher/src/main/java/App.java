import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

public class App {
    public static void main(String[] args) {
        String topic = "iotgroup6/temperature";
        int qos = 2;
        String broker = "tcp://"+args[0]+":1883";
        String clientId = "client_test";
        MemoryPersistence persistence = new MemoryPersistence();

        try {
            MqttClient client = new MqttClient(broker, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setUserName("miguel");
            connOpts.setPassword("miguel".toCharArray());
            connOpts.setCleanSession(true);
            client.connect(connOpts);

            try {
                List<String> stringList = Files.readAllLines(new File("online.data").toPath(), StandardCharsets.UTF_8);
                stringList.remove(0);
                for (String s : stringList) {
                    ObjectMapper mapper = new ObjectMapper();
                    ObjectNode rootNode = mapper.createObjectNode();
                    String [] data = s.split(",");
                    rootNode.put("date", data[0]);
                    rootNode.put("temperature", data[1]);
                    rootNode.put("label", data[2]);
                    String jsonString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(rootNode);
                    MqttMessage message = new MqttMessage(jsonString.getBytes());
                    message.setQos(qos);
                    client.publish(topic, message);
                    System.out.println("Message published with content: " + jsonString);

                    //time to sleep
                    Thread.sleep(10000);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            client.disconnect();
            System.out.println("Disconnected");
            client.close();
            System.exit(0);
        } catch (MqttException me) {
            System.out.println("reason " + me.getReasonCode());
            System.out.println("msg " + me.getMessage());
            System.out.println("loc " + me.getLocalizedMessage());
            System.out.println("cause " + me.getCause());
            System.out.println("excep " + me);
            me.printStackTrace();
        }
    }
}

