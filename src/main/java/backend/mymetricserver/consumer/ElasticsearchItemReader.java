package backend.mymetricserver.reader;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.batch.item.*;

import java.util.*;

@Slf4j
public class ElasticsearchItemReader implements ItemStreamReader<SearchHit> {

    private static final String KEY_SCROLL_ID = "es.scroll.id";

    private final RestHighLevelClient client;
    private final String indexPattern;
    private final Scroll scroll =
            new Scroll(TimeValue.timeValueMinutes(1));

    private String scrollId;
    private Iterator<SearchHit> hitIterator;

    public ElasticsearchItemReader(RestHighLevelClient client, String indexPattern) {
        this.client = client;
        this.indexPattern = indexPattern;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        try {
            // 재시작 여부 확인
            if (executionContext.containsKey(KEY_SCROLL_ID)) {
                scrollId = executionContext.getString(KEY_SCROLL_ID);
                log.info("[ES Reader] Restart detected. Resuming from scrollId={}", scrollId);

                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);

                SearchResponse scrollResponse =
                        client.scroll(scrollRequest, RequestOptions.DEFAULT);

                hitIterator = Arrays.asList(scrollResponse.getHits().getHits()).iterator();
                return;
            }

            // 최초 실행
            log.info("[ES Reader] Initial search on {}", indexPattern);

            SearchRequest searchRequest = new SearchRequest(indexPattern);
            searchRequest.scroll(scroll);
            searchRequest.source(new SearchSourceBuilder()
                    .size(500)
                    .sort("@timestamp") // timestamp sort
            );

            SearchResponse searchResponse =
                    client.search(searchRequest, RequestOptions.DEFAULT);

            scrollId = searchResponse.getScrollId();
            hitIterator = Arrays.asList(searchResponse.getHits().getHits()).iterator();

        } catch (Exception e) {
            throw new ItemStreamException("Failed to open ES reader", e);
        }
    }

    @Override
    public SearchHit read() {
        try {
            if (hitIterator == null)
                return null;

            // 현재 페이지에 남은 데이터가 있다면 그대로 반환
            if (hitIterator.hasNext())
                return hitIterator.next();

            // 다음 scroll 페이지 요청
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);

            SearchResponse scrollResponse =
                    client.scroll(scrollRequest, RequestOptions.DEFAULT);

            // scroll 종료 조건
            if (scrollResponse.getHits().getHits().length == 0)
                return null;

            scrollId = scrollResponse.getScrollId();
            hitIterator = Arrays.asList(scrollResponse.getHits().getHits()).iterator();

            return hitIterator.hasNext() ? hitIterator.next() : null;

        } catch (Exception e) {
            throw new ItemStreamException("Scroll read failed", e);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        // scrollId를 ExecutionContext에 저장 → 재시작 가능
        if (scrollId != null) {
            executionContext.putString(KEY_SCROLL_ID, scrollId);
        }
    }

    @Override
    public void close() {
        if (scrollId == null) return;

        try {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
            log.info("[ES Reader] Scroll cleared. scrollId={}", scrollId);
        } catch (Exception e) {
            log.warn("[ES Reader] Clear scroll failed: {}", e.getMessage());
        }
    }
}
