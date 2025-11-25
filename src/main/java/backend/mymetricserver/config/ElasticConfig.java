package backend.mymetricserver.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticConfig {

    @Bean
    public RestHighLevelClient elasticClient() {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("121.138.215.117", 2171, "http") // 네 curl 엔드포인트 그대로
                )
        );
    }
}
