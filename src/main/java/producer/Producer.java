package producer;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.component.ComponentsBuilderFactory;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.impl.DefaultCamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Producer {
    private static final Logger LOG = LoggerFactory.getLogger(Producer.class);
    private static final String DIRECT_KAFKA_START = "direct:KafkaStart";
    private static final String DIRECT_KAFKA_START_WITH_PARTITIONER = "direct:kafkaStartWithPartitioner";
    // представляет доступ к заголовкам сообщения. В контексте маршрутов Apache Camel заголовки представляют
    // метаданные или дополнительную информацию, связанную с сообщением.
    public static final String HEADERS = "${headers}";

    public static void main(String[] args) throws Exception {
        String testMes = "Test message from producer" + Calendar.getInstance().getTime();
        try (CamelContext camelContext = new DefaultCamelContext()){
            camelContext.getPropertiesComponent().setLocation("classpath:application.properties");
            setUpComponent(camelContext);
            camelContext.addRoutes(createRoutBuider());
            try (ProducerTemplate producerTemplate = camelContext.createProducerTemplate()){
                camelContext.start();
                Map<String, Object> headers = new HashMap<>();
                headers.put(KafkaConstants.PARTITION_KEY, 0);
                headers.put(KafkaConstants.KEY, "1");
                producerTemplate.sendBodyAndHeaders(DIRECT_KAFKA_START, testMes, headers);
                testMes = "TOPIC " + testMes;
                headers.put(KafkaConstants.KEY, "2");
                headers.put(KafkaConstants.TOPIC, "TestLog");
                producerTemplate.sendBodyAndHeaders("direct:kafkaStartNoTopic", testMes, headers);

                testMes = "PART 0 :  " + testMes;
                Map<String, Object> newHeader = new HashMap<>();
                newHeader.put(KafkaConstants.KEY, "AB");
                producerTemplate.sendBodyAndHeaders(DIRECT_KAFKA_START_WITH_PARTITIONER, testMes, newHeader);

                testMes = "PART 1 :  " + testMes;
                newHeader.put(KafkaConstants.KEY, "ABC");
                producerTemplate.sendBodyAndHeaders(DIRECT_KAFKA_START_WITH_PARTITIONER, testMes, newHeader);


            }
            LOG.info("SUCCESSFULLY!");
            Thread.sleep(50_000);


        }

    }

    static RouteBuilder createRoutBuider(){
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from(DIRECT_KAFKA_START).routeId("DirectToKafka")
                        .to("kafka:{{producer.topic}}")
                        .log(HEADERS);

                //Фиктивный топик - это топик, который используется в качестве промежуточной точки при тестировании
                // или отладке приложения, когда реальный топик не требуется или не желателен.
                from("direct:kafkaStartNoTopic").routeId("kafkaStartNoTopic")
                        .to("kafka:dummy")
                        .log(HEADERS);

                from(DIRECT_KAFKA_START_WITH_PARTITIONER).routeId("kafkaStartWithPartitioner")
                        .to("kafka:{{producer.topic}}?partitioner={{producer.partitioner}}")
                        .log(HEADERS);
            }
        };
    }

    static void  setUpComponent(CamelContext camelContext){
        ComponentsBuilderFactory.kafka()
                .brokers("{{kafka.brokers}}")
                .register(camelContext, "kafka");
    }
}
