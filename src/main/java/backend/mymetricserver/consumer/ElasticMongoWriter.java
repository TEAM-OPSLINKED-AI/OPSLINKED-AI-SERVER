package backend.mymetricserver.writer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.elasticsearch.search.SearchHit;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class ElasticMongoWriter implements ItemWriter<SearchHit> {

    private final MongoTemplate mongoTemplate;
    private final String collection;

    @Override
    public void write(List<? extends SearchHit> items) {
        if (items == null || items.isEmpty()) return;

        List<Document> docs = items.stream()
                .map(hit -> {
                    Document doc = new Document(hit.getSourceAsMap());
                    doc.put("_id", hit.getId());
                    doc.put("index", hit.getIndex());
                    doc.put("timestamp", System.currentTimeMillis());
                    return doc;
                })
                .toList();

        mongoTemplate.insert(docs, collection);
        log.info("[ES Writer] Insert {} docs → {}", docs.size(), collection);
    }
}
