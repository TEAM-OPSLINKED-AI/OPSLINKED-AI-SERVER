package backend.mymetricserver.consumer;

import backend.mymetricserver.domain.MetricsDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface MetricsRepository extends MongoRepository<MetricsDocument, String> {
}
