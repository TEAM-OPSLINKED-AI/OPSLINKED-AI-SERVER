package backend.mymetricserver.reader;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.*;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.batch.item.*;

import java.util.Iterator;

@Slf4j
public class ElasticsearchItemReader implements ItemStreamReader<SearchHit> {

    private final RestHighLevelClient client;
    private final String indexPattern;

    private final Scroll scroll = new Scroll(org.elasticsearch.common.unit.TimeValue.timeValueMinutes(1));
    private SearchResponse searchResponse;
    private String scrollId;
    private Iterator<SearchHit> hitIterator;

    public ElasticsearchItemReader(RestHighLevelClient client, String indexPattern) {
        this.client = client;
        this.indexPattern = indexPattern;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        try {
            SearchRequest searchRequest = new SearchRequest(indexPattern);
            searchRequest.scroll(scroll);
            searchRequest.source(new SearchSourceBuilder()
                    .size(500)
                    .sort("@timestamp")
            );

            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            hitIterator = searchResponse.getHits().iterator();

            log.info("[ES Reader] Start scroll on {}", indexPattern);
        } catch (Exception e) {
            throw new ItemStreamException("Failed to open ES reader", e);
        }
    }

    @Override
    public SearchHit read() {
        if (hitIterator == null) return null;

        if (hitIterator.hasNext()) {
            return hitIterator.next();
        }

        try {
            // 다음 scroll 페이지 요청
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);

            SearchResponse scrollResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = scrollResponse.getScrollId();
            hitIterator = scrollResponse.getHits().iterator();

            if (!hitIterator.hasNext()) {
                return null;
            }

            return hitIterator.next();
        } catch (Exception e) {
            throw new ItemStreamException("Scroll read failed", e);
        }
    }

    @Override
    public void close() {
        try {
            if (scrollId != null) {
                ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                clearScrollRequest.addScrollId(scrollId);
                client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
            }
        } catch (Exception e) {
            log.warn("Fail to clear scroll: {}", e.getMessage());
        }
    }
}
