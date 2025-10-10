package backend.mymetricserver.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class BatchJobRunner {

    @Bean
    public CommandLineRunner runJob(JobLauncher jobLauncher, Job metricsJob) {
        return args -> {
            log.info("Starting Spring Batch Job: metricsJob");
            try {
                jobLauncher.run(metricsJob, new JobParameters());
                log.info("metricsJob completed successfully.");
            } catch (Exception e) {
                log.error("Failed to run metricsJob", e);
            }
        };
    }
}