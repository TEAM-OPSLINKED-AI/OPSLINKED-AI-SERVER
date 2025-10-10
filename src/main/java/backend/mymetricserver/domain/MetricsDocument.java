package backend.mymetricserver.domain;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Data
@Document(collection = "mysql_metrics")
public class MetricsDocument {

    @Id
    private String id;

    private String metricName;           // 예: mysql_global_status_commands_total
    private Map<String, String> labels;  // 예: {command=commit}
    private double value;                // 예: 5851
    private long timestamp;              // 수집 시각
}
