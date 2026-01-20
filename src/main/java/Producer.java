import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

// Classe principal do programa
public class Producer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        var producer = new KafkaProducer<String, String>(properties());
        var value = "132123,67523,7894589745";
        var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, value);

        Callback callback = (data, ex) -> {

            if (ex != null){
                ex.printStackTrace(); // Mostra o erro
                return;
            }
            System.out.println(
                    "sucesso enviando "
                            + data.topic()                 // tópico
                            + ":::partition " + data.partition() // partição
                            + "/ offset " + data.offset()        // posição da mensagem
                            + "/ timestamp " + data.timestamp()  // momento do envio
            );
        };
        var email = "Thank you for your order! We are processing your order!";
        var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", email, email);
        producer.send(record, callback).get();
        producer.send(emailRecord, callback).get();
    }

    // Metodo responsável por criar e retornar as configurações do Kafka Producer
    private static Properties properties(){

        // Cria o objeto de configurações
        var properties = new Properties();

        // Endereço do servidor Kafka (broker)
        properties.setProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "127.0.0.1:9092"
        );

        // Define como a chave será serializada (String -> bytes)
        properties.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        // Define como o valor será serializado (String -> bytes)
        properties.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        // Retorna as configurações
        return properties;
    }
}
