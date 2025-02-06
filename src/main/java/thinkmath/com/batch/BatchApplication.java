package thinkmath.com.batch;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.StopWatch;
import thinkmath.com.batch.util.ElasticsearchExecutor;

import java.util.List;

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
        List<String> queryResult = executor.executeNestedSlicedQueries();
        stopWatch.stop();
        long time = stopWatch.getTotalTimeMillis();

        System.out.println("Total time: " + time + "ms");
        System.out.println("Total results: " + queryResult.size());
    }
}
