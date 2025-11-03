package backend.mymetricserver.consumer;

import backend.mymetricserver.domain.MetricsDocument;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.batch.item.ItemProcessor;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class PrometheusLineMetricsProcessor implements ItemProcessor<ConsumerRecord<String, String>, MetricsDocument> {

    // metricName{label="v",label2="v2"} 123.45
    private static final Pattern METRIC_PATTERN = Pattern.compile(
            "(?<name>[a-zA-Z0-9_:]+)(?:\\{(?<labels>[^}]*)\\})?\\s+(?<value>[0-9.eE+-]+)"
    );

    /** Thread-safe Matcher 캐싱 */
    private static final ThreadLocal<Matcher> MATCHER = ThreadLocal.withInitial(() ->
            METRIC_PATTERN.matcher("")
    );

    /** 처리 카운트 (로그 샘플링용) */
    private static final AtomicLong processedCount = new AtomicLong();

    @Override
    public MetricsDocument process(ConsumerRecord<String, String> record) {
        String line = Optional.ofNullable(record.value()).orElse("").trim();
        if (line.isEmpty() || line.startsWith("#")) return null; // 주석 및 공백 제외

        Matcher matcher = MATCHER.get().reset(line);
        if (!matcher.matches()) {
            log.debug("[Processor] Skip (not matched): {}", line);
            return null;
        }

        String name = matcher.group("name");
        String labelStr = matcher.group("labels");
        String valueStr = matcher.group("value");

        double value;
        try {
            value = Double.parseDouble(valueStr);
        } catch (NumberFormatException e) {
            log.warn("[Processor] Invalid numeric value: {}", valueStr);
            return null;
        }

        Map<String, String> labels = new TreeMap<>(parseLabels(labelStr)); // 정렬된 라벨
        MetricsDocument doc = MetricsDocument.builder()
                .metricName(name)
                .labels(labels)
                .value(value)
                .timestamp(System.currentTimeMillis())
                .build();

        long count = processedCount.incrementAndGet();
        if (count % 1000 == 0) {
            log.info("[Processor] Processed {} records so far...", count);
        }

        return doc;
    }

    /**
     * labels 문자열을 안전하게 파싱 (콤마/따옴표 처리)
     * 예) command="insert",db="test",node="ip-10-0-1-23"
     */
    private Map<String, String> parseLabels(String s) {
        Map<String, String> labels = new LinkedHashMap<>();
        if (s == null || s.isEmpty()) return labels;

        List<String> pairs = splitOutsideQuotes(s, ',');

        for (String pair : pairs) {
            int eq = indexOfOutsideQuotes(pair, '=');
            if (eq <= 0) continue;
            try {
                String key = pair.substring(0, eq).trim();
                String rawVal = pair.substring(eq + 1).trim();
                String val = unquote(rawVal);
                labels.put(key, val);
            } catch (Exception e) {
                log.debug("[Processor] Label parse skipped: {}", pair);
            }
        }
        return labels;
    }

    private List<String> splitOutsideQuotes(String s, char sep) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            }
            if (c == sep && !inQuotes) {
                out.add(cur.toString().trim());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        if (cur.length() > 0) out.add(cur.toString().trim());
        return out;
    }

    private int indexOfOutsideQuotes(String s, char ch) {
        boolean inQuotes = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') inQuotes = !inQuotes;
            if (c == ch && !inQuotes) return i;
        }
        return -1;
    }

    private String unquote(String v) {
        String x = v;
        if (x.startsWith("\"") && x.endsWith("\"") && x.length() >= 2) {
            x = x.substring(1, x.length() - 1);
        }
        // Prometheus exporter가 따옴표 내부에서 \" 로 이스케이프할 수 있으므로 복원
        return x.replace("\\\"", "\"");
    }
}
