package backend.mymetricserver.config;

import backend.mymetricserver.consumer.KafkaBatchItemReader;
import backend.mymetricserver.consumer.PrometheusLineMetricsProcessor;
import backend.mymetricserver.domain.MetricsDocument;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Duration;

@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class BatchConfig {

    private static final String TOPIC = "mysql-exporter-metrics";
    private static final String COLLECTION = "mysql_metrics";

    // 추가: node exporter 상수
    private static final String NODE_TOPIC = "node-exporter-metrics";
    private static final String NODE_COLLECTION = "node_metrics";

    private final ConsumerFactory<String, String> consumerFactory;
    private final MongoTemplate mongoTemplate;
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final PlatformTransactionManager transactionManager;

    @Bean
    public Job metricsJob(Step metricsStep, Step nodeStep) { // 변경: nodeStep 추가
        return jobBuilderFactory.get("metricsJob")
                .incrementer(new RunIdIncrementer())
                .start(metricsStep)   // mysql 먼저
                .next(nodeStep)       // node 다음
                .build();
    }

    @Bean
    public Step metricsStep() {
        return stepBuilderFactory.get("metricsStep")
                .<ConsumerRecord<String, String>, MetricsDocument>chunk(200)
                .reader(kafkaItemReader())
                .processor(new PrometheusLineMetricsProcessor())
                .writer(mongoItemWriter())
                .transactionManager(transactionManager)
                .build();
    }

    // 추가: node 스텝
    @Bean
    public Step nodeStep() {
        return stepBuilderFactory.get("nodeStep")
                .<ConsumerRecord<String, String>, MetricsDocument>chunk(200)
                .reader(nodeKafkaItemReader())
                .processor(new PrometheusLineMetricsProcessor())
                .writer(nodeMongoItemWriter())
                .transactionManager(transactionManager)
                .build();
    }

    @Bean
    public ItemReader<ConsumerRecord<String, String>> kafkaItemReader() {
        return new KafkaBatchItemReader(
                consumerFactory,
                TOPIC,
                Duration.ofSeconds(1),
                60,
                false,
                100L
        );
    }

    // 추가: node 리더
    @Bean
    public ItemReader<ConsumerRecord<String, String>> nodeKafkaItemReader() {
        return new KafkaBatchItemReader(
                consumerFactory,
                NODE_TOPIC,
                Duration.ofSeconds(1),
                60,
                false,
                100L
        );
    }

    @Bean
    public ItemWriter<MetricsDocument> mongoItemWriter() {
        return items -> {
            if (items == null || items.isEmpty()) return;
            mongoTemplate.insert(items, COLLECTION);
        };
    }

    // 추가: node 라이터
    @Bean
    public ItemWriter<MetricsDocument> nodeMongoItemWriter() {
        return items -> {
            if (items == null || items.isEmpty()) return;
            mongoTemplate.insert(items, NODE_COLLECTION);
        };
    }
}
