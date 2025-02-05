package thinkmath.com.batch;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import thinkmath.com.batch.util.ElasticsearchExecutor;

import java.io.IOException;

@RequiredArgsConstructor
@Slf4j
@Component
public class ScheduledTasks {
    private final ElasticsearchExecutor executor;

    @Scheduled(fixedRate = 5_000)
    public void reportCurrentTime() throws IOException {
        System.out.println(executor.healthCheck());
    }
}
