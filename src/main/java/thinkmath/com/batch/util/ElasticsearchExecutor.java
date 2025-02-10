package thinkmath.com.batch.util;

import co.elastic.clients.elasticsearch.core.SearchResponse;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
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

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@RequiredArgsConstructor
@Slf4j
@Component
public class ElasticsearchExecutor {
    ElasticsearchService client;
    ExecutorService executorService = Executors.newFixedThreadPool(QueryBuilder.FIXED_THREAD_POOL);

    public boolean healthCheck() throws IOException {
        return client.healthCheck();
    }

    public List<String> executeUseCase(int usecaseNumber) throws IOException {
        SearchResponse<Map> clientQuery =
                client.query(QueryBuilder.CLIENT_INDEX, QueryBuilder.buildClientQueryUsecase1(), true);

        long totalClients = Objects.requireNonNull(clientQuery.hits().total()).value();
        List<Page> clientPaginate = paginate(totalClients, QueryBuilder.BATCH_SIZE);
        String clientPitId = client.getPitId(QueryBuilder.CLIENT_INDEX);
        String eventPitId = client.getPitId(QueryBuilder.EVENT_INDEX);
        List<CompletableFuture<List<String>>> allFutures = new ArrayList<>();
        for (Page clientPage : clientPaginate) {
            CompletableFuture<List<String>> future = CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            return client.executeQueries(
                                    clientPage.pageNumber(), clientPage.size(), usecaseNumber, clientPitId, eventPitId);
                        } catch (IOException e) {
                            throw new CompletionException(e);
                        }
                    },
                    executorService);
            allFutures.add(future);
        }
        return allFutures.stream()
                .map(CompletableFuture::join)
                .flatMap(List::stream)
                .toList();
    }

    private List<Page> paginate(long totalSize, int batchSize) {
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
