package partitoner;

import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class StringPartitioner implements Partitioner {
    @Override
    public void configure(Map<String, ?> configs) {
    }

    //определяет партицию, в которую должно быть отправлено сообщение
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //номер
        int partId = 0;
        // проверяет, является ли ключ экземпляром указанного класса
        if (key instanceof String) {
            //предполагается, что ключ может быть объектом, который может быть приведен к типу String, приведение типов выполняется безопасно.
            String sKey = (String) key;
            //определяется длина строки ключа
            int len = sKey.length();
            //определяет номер партиции, в которую будет отправлено сообщение
            partId = len % 2;
        }

        return partId;
    }

    @Override
    public void close() {
    }
}
