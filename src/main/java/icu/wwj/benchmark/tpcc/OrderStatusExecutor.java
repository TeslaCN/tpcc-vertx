package icu.wwj.benchmark.tpcc;

import icu.wwj.benchmark.tpcc.config.Configurations;
import io.vertx.core.Future;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.Tuple;

import java.util.ArrayList;
import java.util.List;

public class OrderStatusExecutor implements TransactionExecutor {

    private final jTPCCRandom random;

    private final PreparedQuery<RowSet<Row>> stmtOrderStatusSelectCustomerListByLast;

    private final PreparedQuery<RowSet<Row>> stmtOrderStatusSelectCustomer;

    private final PreparedQuery<RowSet<Row>> stmtOrderStatusSelectLastOrder;

    private final PreparedQuery<RowSet<Row>> stmtOrderStatusSelectOrderLine;

    public OrderStatusExecutor(jTPCCRandom random, SqlConnection connection) {
        this.random = random;
        stmtOrderStatusSelectCustomerListByLast = connection.preparedQuery(
                "SELECT c_id " +
                        "    FROM bmsql_customer " +
                        "    WHERE c_w_id = $1 AND c_d_id = $2 AND c_last = $3 " +
                        "    ORDER BY c_first");
        stmtOrderStatusSelectCustomer = connection.preparedQuery(
                "SELECT c_first, c_middle, c_last, c_balance " +
                        "    FROM bmsql_customer " +
                        "    WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3");
        stmtOrderStatusSelectLastOrder = connection.preparedQuery(
                "SELECT o_id, o_entry_d, o_carrier_id " +
                        "    FROM bmsql_oorder " +
                        "    WHERE o_w_id = $1 AND o_d_id = $2 AND o_c_id = $3 " +
                        "      AND o_id = (" +
                        "          SELECT max(o_id) " +
                        "              FROM bmsql_oorder " +
                        "              WHERE o_w_id = $1 AND o_d_id = $2 AND o_c_id = $3" +
                        "          )");
        stmtOrderStatusSelectOrderLine = connection.preparedQuery(
                "SELECT ol_i_id, ol_supply_w_id, ol_quantity, " +
                        "       ol_amount, ol_delivery_d " +
                        "    FROM bmsql_order_line " +
                        "    WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3 " +
                        "    ORDER BY ol_w_id, ol_d_id, ol_o_id, ol_number");
    }

    private OrderStatus generateOrderStatus() {
        OrderStatus orderStatus = new OrderStatus(random.nextInt(1, Configurations.WAREHOUSES), random.nextInt(1, 10));
        if (random.nextInt(1, 100) <= 60) {
            orderStatus.c_id = 0;
            orderStatus.c_last = random.getCLast();
        } else {
            orderStatus.c_id = random.getCustomerID();
            orderStatus.c_last = null;
        }
        return orderStatus;
    }

    @Override
    public Future<Void> execute(Transaction transaction) {
        OrderStatus generated = generateOrderStatus();
        Future<OrderStatus> future = Future.succeededFuture(generated);
        if (null != generated.c_last) {
            future = future.compose(this::selectCustomerListByLast);
        }
        return future.compose(this::selectCustomer)
                .compose(this::selectLastOrder)
                .compose(this::selectOrderLine)
                .eventually(__ -> transaction.rollback());
    }

    private Future<OrderStatus> selectCustomer(OrderStatus orderStatus) {
        return stmtOrderStatusSelectCustomer.execute(Tuple.of(orderStatus.w_id, orderStatus.d_id, orderStatus.c_id))
                .compose(rows -> {
                    if (0 == rows.size()) {
                        throw new IllegalStateException("Customer for C_W_ID=%d C_D_ID=%d C_ID=%d not found".formatted(orderStatus.w_id, orderStatus.d_id, orderStatus.c_id));
                    }
                    Row row = rows.iterator().next();
                    orderStatus.c_first = row.getString("c_first");
                    orderStatus.c_middle = row.getString("c_middle");
                    if (orderStatus.c_last == null) {
                        orderStatus.c_last = row.getString("c_last");
                    }
                    orderStatus.c_balance = row.getDouble("c_balance");
                    return Future.succeededFuture(orderStatus);
                });
    }

    private Future<OrderStatus> selectCustomerListByLast(OrderStatus orderStatus) {
        return stmtOrderStatusSelectCustomerListByLast.execute(Tuple.of(orderStatus.w_id, orderStatus.d_id, orderStatus.c_last))
                .compose(rows -> {
                    if (0 == rows.size()) {
                        throw new IllegalStateException("Customer(s) for C_W_ID=%d C_D_ID=%d C_LAST=%s not found".formatted(orderStatus.w_id, orderStatus.d_id, orderStatus.c_last));
                    }
                    List<Integer> customerIds = new ArrayList<>(rows.size());
                    for (Row row : rows) {
                        customerIds.add(row.getInteger("c_id"));
                    }
                    orderStatus.c_id = customerIds.get((customerIds.size() + 1) / 2 - 1);
                    return Future.succeededFuture(orderStatus);
                });
    }

    private Future<OrderStatus> selectLastOrder(OrderStatus orderStatus) {
        return stmtOrderStatusSelectLastOrder.execute(Tuple.of(orderStatus.w_id, orderStatus.d_id, orderStatus.c_id))
                .compose(rows -> {
                    if (0 == rows.size()) {
                        throw new IllegalStateException("Last Order for W_ID=%d D_ID=%d C_ID=%d not found".formatted(orderStatus.w_id, orderStatus.d_id, orderStatus.c_id));
                    }
                    Row row = rows.iterator().next();
                    orderStatus.o_id = row.getInteger("o_id");
                    orderStatus.o_entry_d = row.getLocalDateTime("o_entry_d");
                    Integer o_carrier_id = row.getInteger("o_carrier_id");
                    orderStatus.o_carrier_id = null == o_carrier_id ? -1 : o_carrier_id;
                    return Future.succeededFuture(orderStatus);
                });
    }

    private Future<Void> selectOrderLine(OrderStatus orderStatus) {
        return stmtOrderStatusSelectOrderLine.execute(Tuple.of(orderStatus.w_id, orderStatus.d_id, orderStatus.o_id))
                .compose(rows -> {
                    int ol_idx = 0;
                    for (Row row : rows) {
                        orderStatus.ol_i_id[ol_idx] = row.getInteger("ol_i_id");
                        orderStatus.ol_supply_w_id[ol_idx] = row.getInteger("ol_supply_w_id");
                        orderStatus.ol_quantity[ol_idx] = row.getInteger("ol_quantity");
                        orderStatus.ol_amount[ol_idx] = row.getDouble("ol_amount");
                        orderStatus.ol_delivery_d[ol_idx] = row.getLocalDateTime("ol_delivery_d");
                        ol_idx++;
                    }
                    for (; ol_idx < 15; ol_idx++) {
                        orderStatus.ol_i_id[ol_idx] = 0;
                        orderStatus.ol_supply_w_id[ol_idx] = 0;
                        orderStatus.ol_quantity[ol_idx] = 0;
                        orderStatus.ol_amount[ol_idx] = 0.0;
                        orderStatus.ol_delivery_d[ol_idx] = null;
                    }
                    return Future.<Void>succeededFuture();
                });
    }
}
