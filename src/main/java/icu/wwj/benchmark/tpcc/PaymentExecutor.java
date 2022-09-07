package icu.wwj.benchmark.tpcc;

import icu.wwj.benchmark.tpcc.config.BenchmarkConfiguration;
import icu.wwj.benchmark.tpcc.sharding.ShardingNumber;
import io.vertx.core.Future;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.Tuple;

import java.time.LocalDateTime;

public class PaymentExecutor implements TransactionExecutor<Void> {
    
    private final BenchmarkConfiguration configuration;
    
    private final jTPCCRandom random;

    public final PreparedQuery<RowSet<Row>> stmtPaymentSelectWarehouse;

    public final PreparedQuery<RowSet<Row>> stmtPaymentSelectDistrict;

    public final PreparedQuery<RowSet<Row>> stmtPaymentSelectCustomerListByLast;

    public final PreparedQuery<RowSet<Row>> stmtPaymentSelectCustomer;

    public final PreparedQuery<RowSet<Row>> stmtPaymentSelectCustomerData;

    public final PreparedQuery<RowSet<Row>> stmtPaymentUpdateWarehouse;

    public final PreparedQuery<RowSet<Row>> stmtPaymentUpdateDistrict;

    public final PreparedQuery<RowSet<Row>> stmtPaymentUpdateCustomer;

    public final PreparedQuery<RowSet<Row>> stmtPaymentUpdateCustomerWithData;

    public final PreparedQuery<RowSet<Row>> stmtPaymentInsertHistory;

    public PaymentExecutor(BenchmarkConfiguration configuration, jTPCCRandom random, SqlConnection connection) {
        this.configuration = configuration;
        this.random = random;
        stmtPaymentSelectWarehouse = connection.preparedQuery(
                "SELECT w_name, w_street_1, w_street_2, w_city, " +
                        "       w_state, w_zip " +
                        "    FROM bmsql_warehouse " +
                        "    WHERE w_id = $1 ");
        stmtPaymentSelectDistrict = connection.preparedQuery(
                "SELECT d_name, d_street_1, d_street_2, d_city, " +
                        "       d_state, d_zip " +
                        "    FROM bmsql_district " +
                        "    WHERE d_w_id = $1 AND d_id = $2");
        stmtPaymentSelectCustomerListByLast = connection.preparedQuery(
                "SELECT c_id " +
                        "    FROM bmsql_customer " +
                        "    WHERE c_w_id = $1 AND c_d_id = $2 AND c_last = $3 " +
                        "    ORDER BY c_first");
        stmtPaymentSelectCustomer = connection.preparedQuery(
                "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, " +
                        "       c_city, c_state, c_zip, c_phone, c_since, c_credit, " +
                        "       c_credit_lim, c_discount, c_balance " +
                        "    FROM bmsql_customer " +
                        "    WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3 " +
                        "    FOR UPDATE");
        stmtPaymentSelectCustomerData = connection.preparedQuery(
                "SELECT c_data " +
                        "    FROM bmsql_customer " +
                        "    WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3");
        stmtPaymentUpdateWarehouse = connection.preparedQuery(
                "UPDATE bmsql_warehouse " +
                        "    SET w_ytd = w_ytd + $1 " +
                        "    WHERE w_id = $2");
        stmtPaymentUpdateDistrict = connection.preparedQuery(
                "UPDATE bmsql_district " +
                        "    SET d_ytd = d_ytd + $1 " +
                        "    WHERE d_w_id = $2 AND d_id = $3");
        stmtPaymentUpdateCustomer = connection.preparedQuery(
                "UPDATE bmsql_customer " +
                        "    SET c_balance = c_balance - $1, " +
                        "        c_ytd_payment = c_ytd_payment + $2, " +
                        "        c_payment_cnt = c_payment_cnt + 1 " +
                        "    WHERE c_w_id = $3 AND c_d_id = $4 AND c_id = $5");
        stmtPaymentUpdateCustomerWithData = connection.preparedQuery(
                "UPDATE bmsql_customer " +
                        "    SET c_balance = c_balance - $1, " +
                        "        c_ytd_payment = c_ytd_payment + $2, " +
                        "        c_payment_cnt = c_payment_cnt + 1, " +
                        "        c_data = $3 " +
                        "    WHERE c_w_id = $4 AND c_d_id = $5 AND c_id = $6");
        stmtPaymentInsertHistory = connection.preparedQuery(
                "INSERT INTO bmsql_history (" +
                        "    h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, " +
                        "    h_date, h_amount, h_data) " +
                        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)");
    }

    @Override
    public Future<Void> execute(Transaction transaction, int warehouseId) {
        Payment generated = generatePayment(warehouseId);
        generated.h_date = LocalDateTime.now();
        Future<Payment> future = Future.succeededFuture(generated);
        // Update the DISTRICT.
        return future.compose(payment -> stmtPaymentUpdateDistrict.execute(Tuple.of(payment.h_amount, payment.w_id, payment.d_id)).map(payment)
        ).compose(payment -> stmtPaymentSelectDistrict.execute(Tuple.of(payment.w_id, payment.d_id))
                // Select the DISTRICT.
                .map(rows -> {
                    if (0 == rows.size()) {
                        throw new IllegalStateException("District for W_ID=%d D_ID=%d not found".formatted(payment.w_id, payment.d_id));
                    }
                    Row row = rows.iterator().next();
                    payment.d_name = row.getString("d_name");
                    payment.d_street_1 = row.getString("d_street_1");
                    payment.d_street_2 = row.getString("d_street_2");
                    payment.d_city = row.getString("d_city");
                    payment.d_state = row.getString("d_state");
                    payment.d_zip = row.getString("d_zip");
                    return payment;
                })
        ).compose(payment -> stmtPaymentUpdateWarehouse.execute(Tuple.of(payment.h_amount, payment.w_id)).map(payment)
                // Update the WAREHOUSE.
        ).compose(payment -> stmtPaymentSelectWarehouse.execute(Tuple.of(payment.w_id))
                // Select the WAREHOUSE.
                .map(rows -> {
                    if (0 == rows.size()) {
                        throw new IllegalStateException("Warehouse for W_ID=%d not found".formatted(payment.w_id));
                    }
                    Row row = rows.iterator().next();
                    payment.w_name = row.getString("w_name");
                    payment.w_street_1 = row.getString("w_street_1");
                    payment.w_street_2 = row.getString("w_street_2");
                    payment.w_city = row.getString("w_city");
                    payment.w_state = row.getString("w_state");
                    payment.w_zip = row.getString("w_zip");
                    return payment;
                })
        ).compose(payment -> null == payment.c_last ? Future.succeededFuture(payment)
                // If C_LAST is given instead of C_ID (60%), determine the C_ID.
                : stmtPaymentSelectCustomerListByLast.execute(Tuple.of(payment.c_w_id, payment.c_d_id, payment.c_last))
                .map(rows -> {
                    if (0 == rows.size()) {
                        throw new IllegalStateException("Customer(s) for C_W_ID=%d C_D_ID=%d C_LAST=%s not found".formatted(payment.c_w_id, payment.c_d_id, payment.c_last));
                    }
                    int i = 0;
                    int[] c_id_list = new int[rows.size()];
                    for (Row row : rows) {
                        c_id_list[i++] = row.getInteger("c_id");
                    }
                    payment.c_id = c_id_list[(c_id_list.length + 1) / 2 - 1];
                    return payment;
                })
        ).compose(payment -> stmtPaymentSelectCustomer.execute(Tuple.of(payment.c_w_id, payment.c_d_id, payment.c_id))
                // Select the CUSTOMER.
                .map(rows -> {
                    if (0 == rows.size()) {
                        throw new IllegalStateException("Customer for C_W_ID=%d C_D_ID=%d C_ID=%d not found".formatted(payment.c_w_id, payment.c_d_id, payment.c_id));
                    }
                    Row row = rows.iterator().next();
                    payment.c_first = row.getString("c_first");
                    payment.c_middle = row.getString("c_middle");
                    if (payment.c_last == null)
                        payment.c_last = row.getString("c_last");
                    payment.c_street_1 = row.getString("c_street_1");
                    payment.c_street_2 = row.getString("c_street_2");
                    payment.c_city = row.getString("c_city");
                    payment.c_state = row.getString("c_state");
                    payment.c_zip = row.getString("c_zip");
                    payment.c_phone = row.getString("c_phone");
                    payment.c_since = row.getLocalDateTime("c_since");
                    payment.c_credit = row.getString("c_credit");
                    payment.c_credit_lim = row.getDouble("c_credit_lim");
                    payment.c_discount = row.getDouble("c_discount");
                    payment.c_balance = row.getDouble("c_balance");
                    payment.c_data = "";
                    payment.c_balance -= payment.h_amount;
                    return payment;
                })
        ).compose(payment -> {
            if (payment.c_credit.equals("GC")) {
                // Customer with good credit, don't update C_DATA.
                return stmtPaymentUpdateCustomer.execute(Tuple.of(payment.h_amount, payment.h_amount, payment.c_w_id, payment.c_d_id, payment.c_id)).map(payment);
            }
            // Customer with bad credit, need to do the C_DATA work.
            return stmtPaymentSelectCustomerData.execute(Tuple.of(payment.c_w_id, payment.c_d_id, payment.c_id))
                    .compose(rows -> {
                        if (0 == rows.size()) {
                            throw new IllegalStateException("Customer.c_data for C_W_ID=%d C_D_ID=%d C_ID=%d not found".formatted(payment.c_w_id, payment.c_d_id, payment.c_id));
                        }
                        Row row = rows.iterator().next();
                        payment.c_data = row.getString("c_data");

                        StringBuilder newData = new StringBuilder("C_ID=%d C_D_ID=%d C_W_ID=%d D_ID=%d W_ID=%d H_AMOUNT=%.2f   ".formatted(
                                payment.c_id, payment.c_d_id, payment.c_w_id, payment.d_id, payment.w_id, payment.h_amount))
                                .append(payment.c_data);
                        if (newData.length() > 500) {
                            newData.setLength(500);
                        }
                        payment.c_data = newData.toString();
                        return stmtPaymentUpdateCustomerWithData.execute(Tuple.of(payment.h_amount, payment.h_amount, payment.c_data, payment.c_w_id, payment.c_d_id, payment.c_id)).map(payment);
                    });
        }).compose(payment -> stmtPaymentInsertHistory.execute(Tuple.of(
                // Insert the HISTORY row.
                payment.c_id,
                payment.c_d_id,
                payment.c_w_id,
                payment.d_id,
                payment.w_id,
                payment.h_date,
                payment.h_amount,
                payment.w_name + "    " + payment.d_name)).mapEmpty()
        ).compose(__ -> transaction.commit(), cause -> transaction.rollback().compose(__ -> Future.failedFuture(cause)));
    }

    private Payment generatePayment(int warehouseId) {
        // 2.5.1.1 & 2.5.1.2
        Payment payment = new Payment(warehouseId, random.nextInt(1, 10));
        payment.c_w_id = payment.w_id;
        payment.c_d_id = payment.d_id;
        if (random.nextInt(1, 100) > 85) {
            payment.c_d_id = random.nextInt(1, 10);
            while (payment.c_w_id == payment.w_id && configuration.getWarehouses() > 1) {
                payment.c_w_id = ShardingNumber.oneSharding(payment.w_id, random.nextInt(1, configuration.getWarehouses()));
            }
        }
        if (random.nextInt(1, 100) <= 60) {
            payment.c_last = random.getCLast();
            payment.c_id = 0;
        } else {
            payment.c_last = null;
            payment.c_id = random.getCustomerID();
        }
        // 2.5.1.3
        payment.h_amount = (double) random.nextLong(100, 500000) / 100.0;
        return payment;
    }
}
