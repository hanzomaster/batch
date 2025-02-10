package thinkmath.com.batch.util;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Component
public class ElasticsearchService implements AutoCloseable {
    public static final Time KEEP_ALIVE = new Time.Builder().time("1m").build();
    private final ElasticsearchClient client;

    public ElasticsearchService() {
        RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200, "https"))
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> {
                    try {
                        return httpAsyncClientBuilder
                                .setDefaultCredentialsProvider(credentialsProvider())
                                .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                                .setSSLContext(createInsecureSSLContext());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .build();

        // Create transport and client
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

        client = new ElasticsearchClient(transport);
    }

    private CredentialsProvider credentialsProvider() {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY, new UsernamePasswordCredentials("elastic", "CJQ+govn44V*D-sNeyHp"));
        return credentialsProvider;
    }

    private SSLContext createInsecureSSLContext() throws Exception {
        SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(
                null,
                new TrustManager[] {
                    new X509TrustManager() {
                        public void checkClientTrusted(X509Certificate[] certs, String authType) {
                            /* TODO document why this method is empty */
                        }

                        public void checkServerTrusted(X509Certificate[] certs, String authType) {
                            /* TODO document why this method is empty */
                        }

                        public X509Certificate[] getAcceptedIssuers() {
                            return new X509Certificate[0];
                        }
                    }
                },
                new SecureRandom());
        return sslContext;
    }

    public boolean healthCheck() throws IOException {
        BooleanResponse ping = client.ping();
        return ping.value();
    }

    public SearchResponse<Map> query(String index, Query query, boolean trackTotalHits) throws IOException {
        if (trackTotalHits) {
            if (query == null) {
                return client.search(s -> s.index(index).trackTotalHits(tth -> tth.enabled(true)), Map.class);
            }
            return client.search(s -> s.index(index).query(query).trackTotalHits(tth -> tth.enabled(true)), Map.class);
        } else {
            if (query == null) {
                return client.search(s -> s.index(index), Map.class);
            }
            return client.search(s -> s.index(index).query(query), Map.class);
        }
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    // TODO: Execute this code
    public List<String> executeQueries(
            int pageNumber, int pageSize, int usecaseNumber, String clientPitId, String eventPitId) throws IOException {
        List<String> clientIds = executeClientsQuery(pageNumber, pageSize, usecaseNumber, clientPitId);
        // return executeEventsQuerySingle(clientIds, usecaseNumber);
        return executeEventsQueryTerms(clientIds, eventPitId);
    }

    private List<String> executeClientsQuery(int pageNumber, int pageSize, int usecaseNumber, String clientPitId)
            throws IOException {
        log.info("Start executing clients query from {} with {} size", pageNumber * pageSize, pageSize);
        StopWatch stopwatch = new StopWatch("executeQueries");
        List<String> clientIds = new ArrayList<>();
        List<FieldValue> searchAfter = null;
        int count = 0;
        boolean infiniteLoop = (pageNumber == -1);

        stopwatch.start("Client query from " + pageNumber + " with size " + pageSize);
        while (infiniteLoop || count++ < pageNumber) {
            SearchRequest.Builder builder = new SearchRequest.Builder()
                    .pit(pit -> pit.id(clientPitId).keepAlive(KEEP_ALIVE))
                    .query(QueryBuilder.buildClientQuery(usecaseNumber))
                    .source(source -> source.filter(
                            filter -> filter.includes(QueryBuilder.CLIENT_ID, "attributes.a_created_date")))
                    .sort(sort -> sort.field(field -> field.field("attributes.a_created_date")));
            if (searchAfter != null) {
                builder = builder.searchAfter(searchAfter);
            }

            SearchResponse<Map> response = client.search(builder.build(), Map.class);
            List<Hit<Map>> hits = response.hits().hits();
            if (hits.isEmpty()) break;

            clientIds.addAll(hits.stream()
                    .map(h -> ((String) Objects.requireNonNull(h.source()).get("client_id")))
                    .toList());

            searchAfter = hits.getLast().sort();
        }

        stopwatch.stop();
        log.info(
                "Client query from {} with size {} took {}ms ~ {}s",
                pageNumber,
                pageSize,
                stopwatch.getTotalTimeMillis(),
                stopwatch.getTotalTimeSeconds());

        return clientIds;
    }

    public List<String> executeEventsQueryTerms(List<String> clientIds, String eventPitId) throws IOException {
        log.info("Start executing events query terms with {} client ids", clientIds.size());
        StopWatch stopwatch = new StopWatch();
        List<FieldValue> searchAfter = null;
        List<String> resultIds = new ArrayList<>();
        stopwatch.start("Event query with terms");

        while (true) {
            SearchRequest.Builder builder = new SearchRequest.Builder()
                    .pit(pit -> pit.id(eventPitId).keepAlive(KEEP_ALIVE))
                    .query(QueryBuilder.buildEventQueryUsecase1(clientIds))
                    .source(source -> source.filter(filter -> filter.includes(QueryBuilder.CLIENT_ID, "@timestamp")))
                    .sort(sort -> sort.field(field -> field.field("@timestamp")));
            if (searchAfter != null) {
                builder = builder.searchAfter(searchAfter);
            }

            SearchResponse<Map> response = client.search(builder.build(), Map.class);
            List<Hit<Map>> hits = response.hits().hits();
            if (hits.isEmpty()) {
                break;
            }
            resultIds.addAll(hits.stream()
                    .map(h -> ((String) Objects.requireNonNull(h.source()).get("client_id")))
                    .toList());

            searchAfter = hits.getLast().sort();
        }

        stopwatch.stop();
        log.info(
                "Event query with terms took {}ms ~ {}s for {} clients",
                stopwatch.getTotalTimeMillis(),
                stopwatch.getTotalTimeSeconds(),
                clientIds.size());

        return resultIds;
    }

    public String getPitId(String clientIndex) throws IOException {
        return client.openPointInTime(b -> b.index(clientIndex).keepAlive(KEEP_ALIVE))
                .id();
    }

    public List<String> executeEventsQuerySingle(List<String> clientIds) throws IOException {
        log.info("Start executing events query single with {} client ids", clientIds.size());
        StopWatch stopwatch = new StopWatch();
        stopwatch.start("Event query with a single client at a time");

        List<String> resultIds = new ArrayList<>();
        for (String clientId : clientIds) {
            SearchRequest.Builder builder = new SearchRequest.Builder()
                    .index(QueryBuilder.EVENT_INDEX)
                    .query(QueryBuilder.buildEventQueryUsecase1(clientId));
            SearchResponse<Map> response = client.search(builder.build(), Map.class);
            if (response.hits().hits().isEmpty()) {
                continue;
            }
            resultIds.add(clientId);
        }

        stopwatch.stop();
        log.info(
                "Event query with a single client at a time took {}ms ~ {}s for {} clients",
                stopwatch.getTotalTimeMillis(),
                stopwatch.getTotalTimeSeconds(),
                clientIds.size());

        return resultIds;
    }
}
