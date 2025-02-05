package thinkmath.com.batch.util;

import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;
import thinkmath.com.batch.dto.QueryResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@RequiredArgsConstructor
@Component
public class ElasticsearchExecutor {
    private final ElasticsearchService client;

    public static <T> Query buildEventQuery(List<T> clientIds) {
        List<FieldValue> clients = clientIds.stream().map(FieldValue::of).toList();
        return Query.of(q -> q.bool(b -> QueryBuilder.getEventLoginPast30Days(b)
                .filter(f -> f.terms(t -> t.field("client_id").terms(ts -> ts.value(clients))))));
    }

    public boolean healthCheck() throws IOException {
        return client.healthCheck();
    }

    public QueryResult executeClientQuery(String indexName, Query query, Integer page) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<Map> allResults = new ArrayList<>();

        try {
            String pitId = client.openPointInTime(indexName);
            List<FieldValue> searchAfter = null;
            long totalHits = 0;
            int count = 0;
            boolean infiniteLoop = (page == null);

            while (infiniteLoop || count++ < page) {
                SearchRequest.Builder builder = new SearchRequest.Builder()
                        .pit(pit -> pit.id(pitId).keepAlive(ElasticsearchService.KEEP_ALIVE))
                        .size(QueryBuilder.BATCH_SIZE)
                        .query(query)
                        .source(source -> source.filter(
                                filter -> filter.includes(QueryBuilder.CLIENT_ID, "attributes.a_created_date")))
                        .sort(sort -> sort.field(field -> field.field("attributes.a_created_date")));
                if (searchAfter != null) {
                    builder = builder.searchAfter(searchAfter);
                }

                SearchRequest searchRequest = builder.build();
                SearchResponse<Map> response = client.queryWithPit(searchRequest);

                List<Hit<Map>> hits = response.hits().hits();
                if (hits.isEmpty()) {
                    break;
                }
                totalHits += hits.size();

                List<Map> list =
                        hits.stream().map(Hit::source).filter(Objects::nonNull).toList();

                allResults.addAll(list);
                searchAfter = hits.get(hits.size() - 1).sort();
            }
            client.closePointInTime(pitId);
            stopWatch.stop();

            long time = stopWatch.getTotalTimeMillis();
            System.out.println(totalHits + " client results in " + time + "ms");
            return new QueryResult(allResults, time, totalHits);
        } catch (IOException e) {
            stopWatch.stop();
            throw new RuntimeException("Failed to execute Elasticsearch query: " + e.getMessage(), e);
        }
    }
    // public <T> QueryResultWithPit<T> executeClientQueryParallel(
    //     String indexName,
    //     Query query,
    //     Class<T> resultClass,
    //     int batch_size,
    //     Integer page) {
    //   StopWatch stopWatch = new StopWatch();
    //   stopWatch.start();
    //   int numberOfSlices = 5;
    //   Queue<String> allResults = new ConcurrentLinkedDeque<>();
    //   AtomicLong totalHits = new AtomicLong(0);
    //
    //   try {
    //     String pitId = client.openPointInTime(indexName);
    //     for (int sliceId = 0; sliceId < numberOfSlices; sliceId++) {
    //       int currentSlice = sliceId;
    //       CompletableFuture<Void> future = CompletableFuture.runAsync()
    //       SearchRequest.Builder builder =
    //           new SearchRequest.Builder().pit(pit -> pit.id(pitId).keepAlive(ElasticsearchService.KEEP_ALIVE))
    //               .slice(s -> s.id(0).max(5))
    //               .size(batch_size)
    //               .query(query)
    //               .source(source -> source.filter(filter -> filter.includes("client_id")))
    //
    //       SearchRequest searchRequest = builder.build();
    //       SearchResponse<T> response = client.queryWithPit(searchRequest, resultClass);
    //
    //       List<Hit<T>> hits = response.hits().hits();
    //       if (hits.isEmpty()) {
    //       }
    //       totalHits += hits.size();
    //
    //       allResults.addAll(hits.stream().map(Hit::source).filter(Objects::nonNull).toList());
    //     }
    //     client.closePointInTime(pitId);
    //     stopWatch.stop();
    //
    //     long time = stopWatch.getTime(TimeUnit.MILLISECONDS);
    //     System.out.println(totalHits + " results in " + time + "ms");
    //     return new QueryResultWithPit<>(allResults, time, totalHits);
    //   } catch (IOException e) {
    //     stopWatch.stop();
    //     throw new RuntimeException("Failed to execute Elasticsearch query: " + e.getMessage(), e);
    //   }
    // }

    public QueryResult executeEventQuery(String indexName, Query query, Integer page) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<Map> allResults = new ArrayList<>();

        try {
            String pitId = client.openPointInTime(indexName);
            List<FieldValue> searchAfter = null;
            long totalHits = 0;
            int count = 0;
            boolean infiniteLoop = (page == null);

            while (infiniteLoop || count++ < page) {
                SearchRequest.Builder builder = new SearchRequest.Builder()
                        .pit(pit -> pit.id(pitId).keepAlive(ElasticsearchService.KEEP_ALIVE))
                        .size(QueryBuilder.BATCH_SIZE)
                        .query(query)
                        .source(source -> source.filter(filter -> filter.includes("client_id", "@timestamp")))
                        .sort(sort -> sort.field(field -> field.field("@timestamp")));
                if (searchAfter != null) builder = builder.searchAfter(searchAfter);
                SearchRequest searchRequest = builder.build();
                SearchResponse<Map> response = client.queryWithPit(searchRequest);
                List<Hit<Map>> hits = response.hits().hits();
                if (hits.isEmpty()) break;
                totalHits += hits.size();
                allResults.addAll(
                        hits.stream().map(Hit::source).filter(Objects::nonNull).toList());
                searchAfter = hits.get(hits.size() - 1).sort();
            }
            client.closePointInTime(pitId);
            stopWatch.stop();
            long time = stopWatch.getTotalTimeMillis();
            System.out.println(totalHits + " event results in " + time + "ms");
            return new QueryResult(allResults, time, totalHits);
        } catch (IOException e) {
            stopWatch.stop();
            throw new RuntimeException("Failed to execute Elasticsearch query: " + e.getMessage(), e);
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
