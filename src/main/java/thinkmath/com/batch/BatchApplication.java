package thinkmath.com.batch;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.StopWatch;
import thinkmath.com.batch.util.ElasticsearchExecutor;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@SpringBootApplication
@EnableScheduling
public class BatchApplication implements CommandLineRunner {
    private final ElasticsearchExecutor executor;

    public static void main(String[] args) {
        SpringApplication.run(BatchApplication.class, args);
    }

    /**
     * Callback used to run the bean.
     *
     * @param args incoming main method arguments
     * @throws Exception on error
     */
    @Override
    public void run(String... args) throws Exception {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        int usecaseNumber = 1;
        List<String> queryResult = executor.executeUseCase(usecaseNumber);
        stopWatch.stop();
        long time = stopWatch.getTotalTimeMillis();

        log.info("Total time: {}ms", time);
        log.info(
                "Total clients: {} with {} distinct clients",
                queryResult.size(),
                queryResult.stream().distinct().count());
    }
}
