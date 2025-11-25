package backend.mymetricserver.consumer;

import backend.mymetricserver.consumer.PrometheusLineMetricsProcessor;
import backend.mymetricserver.domain.MetricsDocument;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class MysqlMetricsConsumer {

    private final MongoTemplate mongoTemplate;
    private final PrometheusLineMetricsProcessor processor = new PrometheusLineMetricsProcessor();

    @KafkaListener(topics = "mysql-exporter-metrics", groupId = "metrics-consumer-live")
    public void consume(ConsumerRecord<String, String> record) {

        MetricsDocument doc = processor.process(record);

        if (doc != null) {
            mongoTemplate.insert(doc, "mysql_metrics");
            log.debug("[MySQL] saved {}", doc.getMetricName());
        }
    }
}
