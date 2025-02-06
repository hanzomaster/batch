package thinkmath.com.batch.util;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;
import thinkmath.com.batch.dto.QueryResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    public boolean healthCheck() throws IOException {
        return client.healthCheck();
    }

    public List<String> executeNestedSlicedQueries() throws IOException, InterruptedException {
        String clientPitId = null;
        String eventPitId = null;
        try {
            clientPitId = client.openPointInTime(QueryBuilder.CLIENT_INDEX);
            eventPitId = client.openPointInTime(QueryBuilder.CLIENT_INDEX);
            List<CompletableFuture<List<String>>> allEventsFutures = new ArrayList<>();

            // Execute first level queries and immediately process their results
            for (int i = 0; i < QueryBuilder.NUMBER_OF_SLICES; i++) {
                int clientCurrentSlice = i;
                String finalClientPitId = clientPitId;
                String finalEventPitId = eventPitId;
                CompletableFuture.supplyAsync(
                                () -> {
                                    try {
                                        return client.executeClientsQuery(
                                                finalClientPitId, clientCurrentSlice, QueryBuilder.NUMBER_OF_SLICES);
                                    } catch (IOException e) {
                                        throw new CompletionException(e);
                                    }
                                },
                                executorService)
                        .thenAccept(clientIds -> {
                            for (int j = 0; j < QueryBuilder.NUMBER_OF_SLICES; j++) {
                                int eventCurrentSlice = j;
                                CompletableFuture<List<String>> secondLevelFuture = CompletableFuture.supplyAsync(
                                        () -> {
                                            try {
                                                return client.executeEventsQuery(
                                                        finalEventPitId,
                                                        clientIds,
                                                        eventCurrentSlice,
                                                        QueryBuilder.NUMBER_OF_SLICES);
                                            } catch (IOException e) {
                                                throw new CompletionException(e);
                                            }
                                        },
                                        executorService);

                                synchronized (allEventsFutures) {
                                    allEventsFutures.add(secondLevelFuture);
                                }
                            }
                        });
            }

            Thread.sleep(200);

            CompletableFuture<List<String>> finalResults = CompletableFuture.allOf(
                            allEventsFutures.toArray(new CompletableFuture[0]))
                    .thenApply(v -> allEventsFutures.stream()
                            .map(CompletableFuture::join)
                            .flatMap(List::stream)
                            .toList());

            return finalResults.join();
        } finally {
            if (clientPitId != null) client.closePointInTime(clientPitId);
            if (eventPitId != null) client.closePointInTime(eventPitId);
            executorService.shutdown();
        }
    }

    public QueryResult executeQuery(String indexName, Query query, boolean trackTotalHits) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        try {
            SearchResponse<Map> response = client.query(indexName, query, trackTotalHits);
            List<Map> hits = response.hits().hits().stream().map(Hit::source).toList();

            stopWatch.stop();
            long executionTime = stopWatch.getTotalTimeMillis();

            long totalHits =
                    response.hits().total() != null ? response.hits().total().value() : 0;
            System.out.println(totalHits + " results in " + executionTime + "ms");
            return new QueryResult(hits, executionTime, totalHits);
        } catch (IOException e) {
            stopWatch.stop();
            throw new RuntimeException("Failed to execute Elasticsearch query: " + e.getMessage(), e);
        }
    }
}
