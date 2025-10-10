package backend.mymetricserver.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class KafkaBatchItemReader implements ItemStreamReader<ConsumerRecord<String, String>>, ItemStream {

    private final org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer;
    private final String topic;

    /** poll 당 대기 시간 */
    private final Duration pollTimeout;

    /** 레코드가 없을 때 최대 재시도 횟수 (총 대기시간 = pollTimeout * maxEmptyPolls) */
    private final int maxEmptyPolls;

    /** 시작 시 토픽 처음부터 읽을지 여부 (컨슈머 그룹에 커밋된 오프셋이 없을 때만 의미 있음) */
    private final boolean seekToBeginningOnOpen;

    /** 이번 실행에서 최대 몇 개만 읽고 끝낼지 (기본 100개) */
    private final long maxTotalRecords;

    private Iterator<ConsumerRecord<String, String>> recordIterator;
    private final AtomicBoolean opened = new AtomicBoolean(false);
    private long emittedCount = 0L;

    /**
     * @param consumerFactory ConsumerFactory
     * @param topic 구독할 토픽
     * @param pollTimeout 각 poll 타임아웃 (예: Duration.ofSeconds(1))
     * @param maxEmptyPolls 비어있는 poll 연속 횟수 한도 (예: 60 -> 최대 60초 대기)
     * @param seekToBeginningOnOpen open 시점에 seekToBeginning 시도 여부
     * @param maxTotalRecords 이번 실행에서 읽을 최대 레코드 수 (예: 100)
     */
    public KafkaBatchItemReader(ConsumerFactory<String, String> consumerFactory,
                                String topic,
                                Duration pollTimeout,
                                int maxEmptyPolls,
                                boolean seekToBeginningOnOpen,
                                Long maxTotalRecords) {
        this.consumer = (org.apache.kafka.clients.consumer.KafkaConsumer<String, String>)
                ((DefaultKafkaConsumerFactory<String, String>) consumerFactory).createConsumer();
        this.topic = topic;
        this.pollTimeout = (pollTimeout != null ? pollTimeout : Duration.ofSeconds(1));
        this.maxEmptyPolls = Math.max(1, maxEmptyPolls);
        this.seekToBeginningOnOpen = seekToBeginningOnOpen;
        this.maxTotalRecords = (maxTotalRecords != null ? Math.max(1L, maxTotalRecords) : 100L);
    }

    /** 기존 생성자 호환: 기본 100개만 읽음 */
    public KafkaBatchItemReader(ConsumerFactory<String, String> consumerFactory,
                                String topic,
                                Duration pollTimeout,
                                int maxEmptyPolls,
                                boolean seekToBeginningOnOpen) {
        this(consumerFactory, topic, pollTimeout, maxEmptyPolls, seekToBeginningOnOpen, 100L);
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        if (opened.compareAndSet(false, true)) {
            consumer.subscribe(Collections.singletonList(topic));
            log.info("[Reader] Subscribed to topic: {}", topic);

            // 최초 poll로 파티션 할당 유도
            consumer.poll(Duration.ofMillis(0));

            if (seekToBeginningOnOpen) {
                try {
                    var assignment = consumer.assignment();
                    if (assignment.isEmpty()) {
                        consumer.poll(Duration.ofSeconds(1)); // 할당 유도
                        assignment = consumer.assignment();
                    }
                    if (!assignment.isEmpty()) {
                        consumer.seekToBeginning(assignment);
                        log.info("[Reader] seekToBeginning applied on assignment: {}", assignment);
                    }
                } catch (Exception e) {
                    log.warn("[Reader] seekToBeginning failed (ignored): {}", e.getMessage());
                }
            }
            emittedCount = 0L;
        }
    }

    @Override
    public ConsumerRecord<String, String> read() {
        // 이미 목표 개수만큼 읽었으면 이번 스텝 종료
        if (emittedCount >= maxTotalRecords) {
            log.info("[Reader] Reached maxTotalRecords={} -> returning null to end step.", maxTotalRecords);
            return null;
        }

        // 기존 버퍼에 남은 레코드가 있으면 우선 반환
        if (recordIterator != null && recordIterator.hasNext()) {
            emittedCount++;
            return recordIterator.next();
        }

        // 없으면 여러 번 poll 하며 대기
        int emptyPolls = 0;
        while (emptyPolls < maxEmptyPolls) {
            ConsumerRecords<String, String> records;
            try {
                records = consumer.poll(pollTimeout);
            } catch (IllegalStateException e) {
                // 이미 닫힌 컨슈머에서 poll 호출되는 경우 방지 로그
                log.warn("[Reader] poll called after consumer closed: {}", e.getMessage());
                return null;
            }

            if (!records.isEmpty()) {
                recordIterator = records.iterator();
                if (recordIterator.hasNext()) {
                    emittedCount++;
                    return recordIterator.next();
                }
            } else {
                emptyPolls++;
            }
        }

        // 최대 대기 후에도 없다면 null → 이번 배치 step 종료
        log.info("[Reader] No records after {} polls (~{} ms). Returning null.",
                maxEmptyPolls, pollTimeout.toMillis() * maxEmptyPolls);
        return null;
    }

    @Override
    public void close() {
        try {
            consumer.close();
            log.info("[Reader] KafkaConsumer closed.");
        } catch (Exception e) {
            log.warn("[Reader] close error: {}", e.getMessage());
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        // 필요 시 주기적 커밋을 원한다면 여기에서 commitSync() 호출 가능
        // 현재는 배치 종료 시점에 커밋(Writer/트랜잭션)되도록 놔둡니다.
    }
}
