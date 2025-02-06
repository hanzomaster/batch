package thinkmath.com.batch.util;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.OpenPointInTimeResponse;
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
        RestClient restClient = RestClient.builder(new HttpHost("127.0.0.1", 9200, "https"))
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

    public String openPointInTime(String index) throws IOException {
        OpenPointInTimeResponse openPointInTimeResponse =
                client.openPointInTime(b -> b.index(index).keepAlive(KEEP_ALIVE));
        return openPointInTimeResponse.id();
    }

    public void closePointInTime(String pitId) throws IOException {
        client.closePointInTime(c -> c.id(pitId));
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    public List<String> executeClientsQuery(String clientPitId, int currentSlice, int numberOfSlices)
            throws IOException {
        log.info(
                "Running client {} slice on total of {} slices with PIT: {}",
                currentSlice,
                numberOfSlices,
                clientPitId);
        Query query = QueryBuilder.buildClientQuery();
        List<FieldValue> searchAfter = null;
        List<String> resultIds = new ArrayList<>();

        while (true) {
            SearchRequest.Builder builder = new SearchRequest.Builder()
                    .pit(pit -> pit.id(clientPitId).keepAlive(KEEP_ALIVE))
                    .slice(s -> s.id(String.valueOf(currentSlice)).max(numberOfSlices))
                    .size(QueryBuilder.BATCH_SIZE)
                    .query(query)
                    .source(source -> source.filter(
                            filter -> filter.includes(QueryBuilder.CLIENT_ID, "attributes.a_created_date")))
                    .sort(sort -> sort.field(field -> field.field("attributes.a_created_date")));
            if (searchAfter != null) builder = builder.searchAfter(searchAfter);

            SearchRequest searchRequest = builder.build();
            SearchResponse<Map> response = queryWithPit(searchRequest);
            List<Hit<Map>> hits = response.hits().hits();
            if (hits.isEmpty()) break;

            resultIds.addAll(hits.stream()
                    .map(h -> ((String) Objects.requireNonNull(h.source()).get("client_id")))
                    .toList());

            searchAfter = hits.get(hits.size() - 1).sort();
        }

        return resultIds;
    }

    private SearchResponse<Map> queryWithPit(SearchRequest query) throws IOException {
        return client.search(query, Map.class);
    }

    public List<String> executeEventsQuery(
            String eventPitId, List<String> clientIds, int currentSlice, int numberOfSlices) throws IOException {
        log.info("Running event {} slice on total of {} slices with PIT: {}", currentSlice, numberOfSlices, eventPitId);
        Query query = QueryBuilder.buildEventQuery(clientIds);
        List<FieldValue> searchAfter = null;
        List<String> resultIds = new ArrayList<>();

        while (true) {
            SearchRequest.Builder builder = new SearchRequest.Builder()
                    .pit(pit -> pit.id(eventPitId).keepAlive(KEEP_ALIVE))
                    .slice(s -> s.id(String.valueOf(currentSlice)).max(numberOfSlices))
                    .size(QueryBuilder.BATCH_SIZE)
                    .query(query)
                    .source(source -> source.filter(filter -> filter.includes(QueryBuilder.CLIENT_ID, "@timestamp")))
                    .sort(sort -> sort.field(field -> field.field("@timestamp")));
            if (searchAfter != null) builder = builder.searchAfter(searchAfter);

            SearchRequest searchRequest = builder.build();
            SearchResponse<Map> response = queryWithPit(searchRequest);
            List<Hit<Map>> hits = response.hits().hits();
            if (hits.isEmpty()) break;

            resultIds.addAll(hits.stream()
                    .map(h -> ((String) Objects.requireNonNull(h.source()).get("client_id")))
                    .toList());

            searchAfter = hits.get(hits.size() - 1).sort();
        }

        return resultIds;
    }
}
