package thinkmath.com.batch.util;

import co.elastic.clients.elasticsearch.core.SearchResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.datafaker.Faker;
import org.springframework.stereotype.Component;
import thinkmath.com.batch.dto.Page;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@RequiredArgsConstructor
@Component
public class ElasticsearchExecutor {
    private final ElasticsearchService client;
    private final ExecutorService executorService =
            Executors.newFixedThreadPool(QueryBuilder.NUMBER_OF_SLICES * QueryBuilder.NUMBER_OF_SLICES);
    private final Faker faker = new Faker();

    public boolean healthCheck() throws IOException {
        return client.healthCheck();
    }

    public List<String> executeUseCase() throws IOException {
        SearchResponse<Map> clientQuery =
                client.query(QueryBuilder.CLIENT_INDEX, QueryBuilder.buildClientQuery(), true);
        long totalClients = Objects.requireNonNull(clientQuery.hits().total()).value();
        System.out.println(totalClients);
        List<Page> clientPaginate = paginate(totalClients, QueryBuilder.BATCH_SIZE);
        List<CompletableFuture<List<String>>> allFutures = new ArrayList<>();
        for (Page clientPage : clientPaginate) {
            log.info("Executing client page {}", clientPage.pageNumber());
            CompletableFuture<List<String>> future = CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return client.executeClientsQuery(clientPage.pageNumber(), clientPage.size());
                                } catch (IOException e) {
                                    throw new CompletionException(e);
                                }
                            },
                            executorService)
                    .thenApply(clientIds -> {
                        try {
                            return client.executeEventsQueryTerms(clientIds);
                            //                            return client.executeEventsQuerySingle(clientIds);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
            allFutures.add(future);
        }
        CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0])).join();
        return new ArrayList<>();
    }

    private static List<Page> paginate(long totalSize, int batchSize) {
        List<Page> pages = new ArrayList<>();
        int pageNumber = 0;
        while (totalSize > 0) {
            int currentBatchSize = (int) Math.min(batchSize, totalSize);
            pages.add(new Page(pageNumber++, currentBatchSize));
            totalSize -= currentBatchSize;
        }
        return pages;
    }
}
