package backend.mymetricserver.consumer;

import backend.mymetricserver.domain.MetricsDocument;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.batch.item.ItemProcessor;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class PrometheusLineMetricsProcessor implements ItemProcessor<ConsumerRecord<String, String>, MetricsDocument> {

    // metricName{label="v",label2="v2"} 123.45
    private static final Pattern METRIC_PATTERN = Pattern.compile(
            "(?<name>[a-zA-Z0-9_:]+)(?:\\{(?<labels>[^}]*)\\})?\\s+(?<value>[0-9.eE+-]+)"
    );

    @Override
    public MetricsDocument process(ConsumerRecord<String, String> record) {
        final String line = record.value() == null ? "" : record.value().trim();
        if (line.isEmpty()) return null;

        Matcher m = METRIC_PATTERN.matcher(line);
        if (!m.matches()) {
            log.debug("[Processor] Skip (not matched): {}", line);
            return null;
        }

        String name = m.group("name");
        String labelStr = m.group("labels");
        String valueStr = m.group("value");

        Map<String, String> labels = parseLabels(labelStr);
        double value = Double.parseDouble(valueStr);

        MetricsDocument doc = MetricsDocument.builder()
                .metricName(name)
                .labels(labels)
                .value(value)
                .timestamp(System.currentTimeMillis())
                .build();

        log.debug("[Processor] Parsed -> {}", doc);
        return doc;
    }

    /**
     * labels 문자열을 안전하게 파싱 (콤마/따옴표 처리)
     * 예) command="insert",db="test",node="ip-10-0-1-23"
     */
    private Map<String, String> parseLabels(String s) {
        Map<String, String> labels = new LinkedHashMap<>();
        if (s == null || s.isEmpty()) return labels;

        // 콤마를 따옴표 밖에서만 분리
        List<String> pairs = splitOutsideQuotes(s, ',');

        for (String pair : pairs) {
            int eq = indexOfOutsideQuotes(pair, '=');
            if (eq <= 0) continue;

            String key = pair.substring(0, eq).trim();
            String rawVal = pair.substring(eq + 1).trim();
            String val = unquote(rawVal);
            labels.put(key, val);
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
                cur.append(c);
            } else if (c == sep && !inQuotes) {
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
