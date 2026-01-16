import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

// Classe principal do programa
public class NewOrderMain {

    // Metodo principal (ponto de entrada da aplicação)
    // Lança exceções porque usamos send().get(), que pode gerar erros de execução
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // variavel que referencia um objeto e cria um Producer Kafka
        // <String, String> indica o tipo da chave e do valor da mensagem
        // properties() retorna as configurações do Kafka
        var producer = new KafkaProducer<String, String>(properties());

        // Conteúdo da mensagem (exemplo: pedidoId, clienteId, produtoId)
        var value = "132123,67523,7894589745";

        // Cria a mensagem que será enviada ao Kafka
        // "ECOMMERCE_NEW_ORDER" -> nome do tópico
        // value -> chave da mensagem (define a partição)
        // value -> valor da mensagem (conteúdo)
        var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, value);

        // Envia a mensagem para o Kafka
        // O envio é assíncrono e recebe um callback
        producer.send(record, (data, ex) -> {

            // Se ocorreu algum erro durante o envio
            if (ex != null){
                ex.printStackTrace(); // Mostra o erro
                return;
            }

            // Caso o envio tenha sido bem-sucedido
            // Exibe informações importantes da mensagem enviada
            System.out.println(
                    "sucesso enviando "
                            + data.topic()                 // tópico
                            + ":::partition " + data.partition() // partição
                            + "/ offset " + data.offset()        // posição da mensagem
                            + "/ timestamp " + data.timestamp()  // momento do envio
            );

            // .get() faz o programa esperar o envio finalizar
            // Transforma o envio assíncrono em síncrono
        }).get();
    }

    // Método responsável por criar e retornar as configurações do Kafka Producer
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
