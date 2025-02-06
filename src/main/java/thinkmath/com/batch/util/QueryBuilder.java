package thinkmath.com.batch.util;

import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.DateRangeQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.util.ObjectBuilder;
import lombok.experimental.UtilityClass;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

@UtilityClass
public class QueryBuilder {
    public final int BATCH_SIZE = 10_000;
    public final String CLIENT_INDEX = "clients-*";
    public final String EVENT_INDEX = "events-*";
    public final String CLIENT_ID = "client_id";
    public final int NUMBER_OF_SLICES = 5;

    public Query buildClientQuery() {
        return Query.of(excludeCertainCustomers());
    }

    public Function<Query.Builder, ObjectBuilder<Query>> excludeCertainCustomers() {
        return q -> q.bool(b -> b.must(m -> m.bool(innerBool -> innerBool
                        .should(s -> s.range(r -> r.term(
                                y -> y.field("attributes.a_c_ins_loyalty_point").lt("1000"))))
                        .should(s -> s.bool(
                                b2 -> b2.mustNot(mn -> mn.exists(e -> e.field("attributes.a_c_ins_loyalty_point")))))
                        .minimumShouldMatch("1")))
                .filter(m -> m.bool(innerBool -> innerBool
                        .mustNot(mn -> mn.term(
                                t -> t.field("attributes.a_custom_segment_id").value("240")))
                        .mustNot(mn -> mn.term(
                                t -> t.field("attributes.a_custom_segment_id").value("981")))
                        .mustNot(mn -> mn.terms(t -> t.field("attributes.a_c_ins_customer_level")
                                .terms(terms -> terms.value(Arrays.asList(
                                        FieldValue.of("9"),
                                        FieldValue.of("10"),
                                        FieldValue.of("11"),
                                        FieldValue.of("12"),
                                        FieldValue.of("13"),
                                        FieldValue.of("14"),
                                        FieldValue.of("15")))))))));
    }

    public Query buildEventQuery() {
        return Query.of(q -> q.bool(QueryBuilder::getEventLoginPast30Days));
    }

    public BoolQuery.Builder getEventLoginPast30Days(BoolQuery.Builder b) {
        return b.must(m -> m.term(t -> t.field("event_name").value("ins_dangnhap_success")))
                .mustNot(mn -> mn.bool(
                        bn -> bn.must(m -> m.term(t -> t.field("event_name").value("ins_dangnhap_success")))
                                .must(m -> m.range(r -> r.date(DateRangeQuery.of(d ->
                                        d.field("@timestamp").gte("now-15d/d").lt("now/d")))))))
                .filter(f -> f.range(r -> r.date(DateRangeQuery.of(
                        d -> d.field("@timestamp").gte("now-30d/d").lt("now-15d/d")))));
    }

    public Query buildEventQuery(List<String> clientIds) {
        List<FieldValue> clients = clientIds.stream().map(FieldValue::of).toList();
        return Query.of(q -> q.bool(b -> getEventLoginPast30Days(b)
                .filter(f -> f.terms(t -> t.field(CLIENT_ID).terms(ts -> ts.value(clients))))));
    }
    public Query buildEventQuery(String clientId) {
        return Query.of(q -> q.bool(b -> getEventLoginPast30Days(b)
                .filter(f -> f.term(t -> t.field(CLIENT_ID).value(clientId)))));
    }
}
