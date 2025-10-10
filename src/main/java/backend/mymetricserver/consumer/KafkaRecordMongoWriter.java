package backend.mymetricserver.writer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.bson.Document;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class KafkaRecordMongoWriter implements ItemWriter<ConsumerRecord<String, String>> {

    private final MongoTemplate mongoTemplate;
    private final String collectionName; // "mysql_metrics"

    @Override
    public void write(List<? extends ConsumerRecord<String, String>> items) {
        if (items == null || items.isEmpty()) return;

        List<Document> docs = new ArrayList<>(items.size());
        for (ConsumerRecord<String, String> r : items) {
            Document d = new Document()
                    .append("topic", r.topic())
                    .append("partition", r.partition())
                    .append("offset", r.offset())
                    .append("timestamp", r.timestamp())
                    .append("key", r.key())
                    .append("value", r.value());
            docs.add(d);
        }

        mongoTemplate.insert(docs, collectionName);
        log.info("[Writer] inserted {} docs into '{}'", docs.size(), collectionName);
    }
}
