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
    public static final int FIXED_THREAD_POOL = 3;

    public Query buildClientQuery(int usecaseNumber) {
        return switch (usecaseNumber) {
            case 1 -> buildClientQueryUsecase1();
            case 2 -> buildClientQueryUsecase2();
            case 3 -> buildClientQueryUsecase3();
            default -> throw new IllegalStateException("This use case doesn't have a query: " + usecaseNumber);
        };
    }

    public Query buildClientQueryUsecase1() {
        return Query.of(excludeCertainCustomers());
    }

    public Query buildClientQueryUsecase2() {
        return Query.of(excludeCertainCustomers());
    }

    public Query buildClientQueryUsecase3() {
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

    public Query buildEventQueryUsecase1(List<String> clientIds) {
        List<FieldValue> clients = clientIds.stream().map(FieldValue::of).toList();
        return Query.of(q -> q.bool(b -> getEventLoginPast30Days(b)
                .filter(f -> f.terms(t -> t.field(CLIENT_ID).terms(ts -> ts.value(clients))))));
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

    public Query buildEventQueryUsecase1(String clientId) {
        return Query.of(q -> q.bool(b -> getEventLoginPast30Days(b)
                .filter(f -> f.term(t -> t.field(CLIENT_ID).value(clientId)))));
    }

    public Query buildEventQueryUsecase2(List<String> clientIds) {
        List<FieldValue> clients = clientIds.stream().map(FieldValue::of).toList();
        return Query.of(q -> q.bool(b -> getVietQRNotDisbursed(b)
                .filter(f -> f.terms(t -> t.field(CLIENT_ID).terms(ts -> ts.value(clients))))
        ));
    }

    private static BoolQuery.Builder getVietQRNotDisbursed(BoolQuery.Builder b) {
        return b
                .must(m -> m.bool(innerBool -> innerBool
                        .must(m1 -> m1.bool(b1 -> b1
                                .must(mt -> mt.term(t -> t.field("event_name").value("ins_vay_giaingan_success")))
                                .must(mt -> mt.range(r -> r.date(DateRangeQuery.of(d -> d.field("@timestamp").lt("now-30d")))))
                                .must(mt -> mt.match(m2 -> m2.field("ins_vay_success_term").query("tháng")))
                                .must(mt -> mt.match(m2 -> m2.field("ins_vay_success_type").query("Tín dụng VietQR")))
                        ))
                        .must(m1 -> m1.bool(b1 -> b1
                                .must(mt -> mt.term(t -> t.field("event_name").value("ins_vay_dangky_success")))
                                .must(mt -> mt.range(r -> r.date(DateRangeQuery.of(d -> d.field("@timestamp").gte("now-30d")))))
                                .must(mt -> mt.match(m2 -> m2.field("ins_vay_success_type").query("Tín dụng VietQR")))
                                .must(mt -> mt.match(m2 -> m2.field("ins_vay_success_type").query("tháng")))
                        ))
                ))
                .should(s -> s.bool(b1 -> b1
                        .must(mt -> mt.term(t -> t.field("event_name").value("push_delivered")))
                        .must(mt -> mt.range(r -> r.date(DateRangeQuery.of(d -> d.field("@timestamp").gte("now-30d")))))
                        .must(mt -> mt.term(t -> t.field("e_journey_id").value("1861")))
                ))
                .should(s -> s.bool(b1 -> b1
                        .must(mt -> mt.term(t -> t.field("event_name").value("push_delivered")))
                        .must(mt -> mt.range(r -> r.date(DateRangeQuery.of(d -> d.field("@timestamp").gte("now-30d")))))
                        .must(mt -> mt.term(t -> t.field("e_journey_id").value("1865")))
                ))
                .minimumShouldMatch(String.valueOf(1));
    }

    public Query buildEventQueryUsecase2(String clientId) {
        return Query.of(q -> q.bool(b -> getVietQRNotDisbursed(b)
                .filter(f -> f.term(t -> t.field(CLIENT_ID).value(clientId)))));
    }

    public Query buildEventQueryUsecase3(List<String> clientIds) {
        List<FieldValue> clients = clientIds.stream().map(FieldValue::of).toList();
        return Query.of(q -> q.bool(b -> getNotYetInstallmentPayment(b)
                .filter(f -> f.terms(t -> t.field(CLIENT_ID).terms(ts -> ts.value(clients))))
        ));
    }

    private static BoolQuery.Builder getNotYetInstallmentPayment(BoolQuery.Builder b) {
            return b
                .must(m -> m.bool(innerBool -> innerBool
                        // Transaction conditions
                        .must(m1 -> m1.bool(b1 -> b1
                                .must(mt -> mt.term(t -> t.field("event_name").value("ins_the_giaodich")))
                                .must(mt -> mt.range(r -> r.date(DateRangeQuery.of(d -> d.field("@timestamp").gte("now-1d")))))
                                .must(mt -> mt.range(r -> r.date(DateRangeQuery.of(d -> d.field("@timstamp").gte("2500000")))))
                                .must(mt -> mt.range(r -> r.date(DateRangeQuery.of(d -> d.field("@timestamp").lte("3000000")))))
                        ))
                        // Credit card attribute
                        .must(m1 -> m1.match(t -> t.field("a_c_ins_credit_card").query("True")))
                        // No installment success in last 30 days
                        .must(m1 -> m1.bool(b1 -> b1
                                .must(mt -> mt.term(t -> t.field("event_name").value("ins_card_tragop_success")))
                                .must(mt -> mt.range(r -> r.date(DateRangeQuery.of(d -> d.field("@timestamp").lt("now-30d")))))
                        ))
                ));
    }

    public Query buildEventQueryUsecase3(String clientId) {
        return Query.of(q -> q.bool(b -> getNotYetInstallmentPayment(b)
                .filter(f -> f.term(t -> t.field(CLIENT_ID).value(clientId)))));
    }
}
