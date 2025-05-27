import io.camunda.zeebe.client.CredentialsProvider;
import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.response.ProcessInstanceEvent;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.HashMap;
import java.util.Map;

public class KafkaToCamundaStarter {

    public static void main(String[] args) {

        // CAMUNDA 8 client
        final URI CAMUNDA_CLIENT_ZEEBE_GRPCADDRESS = URI
                .create("grpcs://b1c5e41e-ccdf-49d9-bffa-976b890d4598.bru-2.zeebe.camunda.io:443");
        final URI CAMUNDA_CLIENT_ZEEBE_RESTADDRESS = URI
                .create("https://bru-2.zeebe.camunda.io/b1c5e41e-ccdf-49d9-bffa-976b890d4598");
        final String CAMUNDA_CLIENT_AUTH_CLIENTID = "9S1woVJpsYhjr5dy7nHP_8Y9oHM4iFK_";
        final String CAMUNDA_CLIENT_AUTH_CLIENTSECRET = "TODO";
        final String CAMUNDA_CLIENT_AUTH_AUDIENCE = "zeebe.camunda.io";
        final String CAMUNDA_CLIENT_AUTH_TOKENURL = "https://login.cloud.camunda.io/oauth/token";
        
        CredentialsProvider provider = CredentialsProvider.newCredentialsProviderBuilder()
                .clientId(CAMUNDA_CLIENT_AUTH_CLIENTID)
                .clientSecret(CAMUNDA_CLIENT_AUTH_CLIENTSECRET)
                .audience(CAMUNDA_CLIENT_AUTH_AUDIENCE)
                .authorizationServerUrl(CAMUNDA_CLIENT_AUTH_TOKENURL)
                .build();

        final ZeebeClient client = ZeebeClient.newClientBuilder()
                .grpcAddress(CAMUNDA_CLIENT_ZEEBE_GRPCADDRESS)
                .restAddress(CAMUNDA_CLIENT_ZEEBE_RESTADDRESS)
                .credentialsProvider(provider)
                .build();

        // Kafka client setup
        Properties props = new Properties();
        props.put("bootstrap.servers", "d0ioobo4t9qgkqm62f70.any.eu-central-1.mpx.prd.cloud.redpanda.com:9092");
        props.put("group.id", "camunda-consumer-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");

        // SASL Authentication
        props.put("client.id", "POCClient");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                        "username=\"process\" password=\"TODO\";");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("spm-events"));

            while (true) {
                System.out.println("Polling");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
                for (ConsumerRecord<String, String> record : records) {
                    String message = record.value();
                    System.out.println("Message: /n" + message + "/n");
                    System.out.println("Received Kafka message: " + message);

                    // Start a Camunda process instance with the message as a variable
                    Map<String, Object> variables = new HashMap<>();
                    variables.put("kafkaMessage", message);

                    ProcessInstanceEvent instance = client
                            .newCreateInstanceCommand()
                            .bpmnProcessId("kafkaExample") // Your BPMN process ID
                            .latestVersion()
                            .variables(variables)
                            .send()
                            .join();

                    System.out.println("Started process instance: " + instance.getProcessInstanceKey());
                }
            }
        }
    }

}
