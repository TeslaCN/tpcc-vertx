package icu.wwj.benchmark.tpcc;

import icu.wwj.benchmark.tpcc.config.BenchmarkConfiguration;
import io.vertx.core.Future;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.Tuple;

import java.time.LocalDateTime;
import java.util.Arrays;

public class DeliveryExecutor implements TransactionExecutor<Void> {
    
    private final BenchmarkConfiguration configuration;

    private final jTPCCRandom random;

    private final PreparedQuery<RowSet<Row>> stmtDeliveryBGSelectOldestNewOrder;

    private final PreparedQuery<RowSet<Row>> stmtDeliveryBGDeleteOldestNewOrder;

    private final PreparedQuery<RowSet<Row>> stmtDeliveryBGSelectOrder;

    private final PreparedQuery<RowSet<Row>> stmtDeliveryBGUpdateOrder;

    private final PreparedQuery<RowSet<Row>> stmtDeliveryBGSelectSumOLAmount;

    private final PreparedQuery<RowSet<Row>> stmtDeliveryBGUpdateOrderLine;

    private final PreparedQuery<RowSet<Row>> stmtDeliveryBGUpdateCustomer;

    public DeliveryExecutor(BenchmarkConfiguration configuration, jTPCCRandom random, SqlConnection connection) {
        this.configuration = configuration;
        this.random = random;
        stmtDeliveryBGSelectOldestNewOrder = connection.preparedQuery(
                "SELECT no_o_id " +
                        "    FROM bmsql_new_order " +
                        "    WHERE no_w_id = $1 AND no_d_id = $2 " +
                        "    ORDER BY no_o_id ASC limit 1");
        stmtDeliveryBGDeleteOldestNewOrder = connection.preparedQuery(
                "DELETE FROM bmsql_new_order " +
                        "    WHERE no_w_id = $1 AND no_d_id = $2 AND no_o_id = $3");
        stmtDeliveryBGSelectOrder = connection.preparedQuery(
                "SELECT o_c_id " +
                        "    FROM bmsql_oorder " +
                        "    WHERE o_w_id = $1 AND o_d_id = $2 AND o_id = $3");
        stmtDeliveryBGUpdateOrder = connection.preparedQuery(
                "UPDATE bmsql_oorder " +
                        "    SET o_carrier_id = $1 " +
                        "    WHERE o_w_id = $2 AND o_d_id = $3 AND o_id = $4");
        stmtDeliveryBGSelectSumOLAmount = connection.preparedQuery(
                "SELECT sum(ol_amount) AS sum_ol_amount " +
                        "    FROM bmsql_order_line " +
                        "    WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3");
        stmtDeliveryBGUpdateOrderLine = connection.preparedQuery(
                "UPDATE bmsql_order_line " +
                        "    SET ol_delivery_d = $1 " +
                        "    WHERE ol_w_id = $2 AND ol_d_id = $3 AND ol_o_id = $4");
        stmtDeliveryBGUpdateCustomer = connection.preparedQuery(
                "UPDATE bmsql_customer " +
                        "    SET c_balance = c_balance + $1, " +
                        "        c_delivery_cnt = c_delivery_cnt + 1 " +
                        "    WHERE c_w_id = $2 AND c_d_id = $3 AND c_id = $4");
    }

    @Override
    public Future<Void> execute(Transaction transaction, final int warehouseId) {
        Delivery generated = generateDelivery(warehouseId);
        return Future.succeededFuture(generated)
                .compose(delivery -> handleDistrictId(delivery, 1))
                .compose(delivery -> handleDistrictId(delivery, 2))
                .compose(delivery -> handleDistrictId(delivery, 3))
                .compose(delivery -> handleDistrictId(delivery, 4))
                .compose(delivery -> handleDistrictId(delivery, 5))
                .compose(delivery -> handleDistrictId(delivery, 6))
                .compose(delivery -> handleDistrictId(delivery, 7))
                .compose(delivery -> handleDistrictId(delivery, 8))
                .compose(delivery -> handleDistrictId(delivery, 9))
                .compose(delivery -> handleDistrictId(delivery, 10))
                .compose(__ -> transaction.commit(), cause -> transaction.rollback().compose(unused -> Future.failedFuture(cause)));
    }

    private Delivery generateDelivery(int warehouseId) {
        Delivery delivery = new Delivery(warehouseId);
        delivery.ol_delivery_d = LocalDateTime.now();
        delivery.o_carrier_id = random.nextInt(1, 10);
        Arrays.fill(delivery.delivered_o_id, -1);
        return delivery;
    }

    private Future<Delivery> handleDistrictId(Delivery delivery, int d_id) {
        return getOid(delivery, d_id).compose(o_id -> o_id < 0 ? Future.succeededFuture(delivery) : handleOid(delivery, d_id, o_id));
    }

    private Future<Integer> getOid(Delivery delivery, int d_id) {
        return stmtDeliveryBGSelectOldestNewOrder.execute(Tuple.of(delivery.w_id, d_id)).compose(rows -> {
            if (0 == rows.size()) {
                return Future.succeededFuture(-1);
            }
            int o_id = rows.iterator().next().getInteger("no_o_id");
            return stmtDeliveryBGDeleteOldestNewOrder.execute(Tuple.of(delivery.w_id, d_id, o_id))
                    /*
                     * Failed to delete the NEW_ORDER row. This is not
                     * an error since for concurrency reasons we did
                     * not select FOR UPDATE above. It is possible that
                     * another, concurrent DELIVERY_BG transaction just
                     * deleted this row and is working on it now. We
                     * simply got back and try to get the next one.
                     * This logic only works in READ_COMMITTED isolation
                     * level and will cause SQLExceptions in anything
                     * higher than that.
                     */
                    .compose(updateResult -> 0 == updateResult.rowCount() ? getOid(delivery, d_id) : Future.succeededFuture(o_id));
        });
    }

    /**
     * We found the oldest undelivered order for this DISTRICT
     * and the NEW_ORDER line has been deleted. Process the
     * rest of the DELIVERY_BG.
     */
    private Future<Delivery> handleOid(Delivery delivery, int d_id, int o_id) {
        // Update the ORDER setting the o_carrier_id.
        return stmtDeliveryBGUpdateOrder.execute(Tuple.of(delivery.o_carrier_id, delivery.w_id, d_id, o_id))
                // Get the o_c_id from the ORDER.
                .compose(__ -> stmtDeliveryBGSelectOrder.execute(Tuple.of(delivery.w_id, d_id, o_id)).map(rows -> {
                    if (0 == rows.size()) {
                        throw new IllegalStateException("ORDER in DELIVERY_BG for O_W_ID=%d O_D_ID=%d O_ID=%d not found".formatted(delivery.w_id, d_id, o_id));
                    }
                    return rows.iterator().next().getInteger("o_c_id");
                }))
                // Update ORDER_LINE setting the ol_delivery_d.
                // TODO Using same LocalDateTime instead of LocalDateTime.now()
                .compose(o_c_id -> stmtDeliveryBGUpdateOrderLine.execute(Tuple.of(LocalDateTime.now(), delivery.w_id, d_id, o_id))
                        // Select the sum(ol_amount) from ORDER_LINE.
                        .compose(__ -> stmtDeliveryBGSelectSumOLAmount.execute(Tuple.of(delivery.w_id, d_id, o_id)).map(rows -> {
                            if (0 == rows.size()) {
                                throw new IllegalStateException("sum(OL_AMOUNT) for ORDER_LINEs with OL_W_ID=%d OL_D_ID=%d OL_O_ID=%d not found".formatted(delivery.w_id, d_id, o_id));
                            }
                            return rows.iterator().next().getDouble("sum_ol_amount");
                            // Update the CUSTOMER.
                        }).compose(sum_ol_amount -> stmtDeliveryBGUpdateCustomer.execute(Tuple.of(sum_ol_amount, delivery.w_id, d_id, o_c_id)))))
                .map(__ -> {
                    delivery.delivered_o_id[d_id - 1] = o_id;
                    return delivery;
                });
    }
}
