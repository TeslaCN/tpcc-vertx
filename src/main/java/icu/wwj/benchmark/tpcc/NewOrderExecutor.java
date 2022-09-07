package icu.wwj.benchmark.tpcc;

import icu.wwj.benchmark.tpcc.config.BenchmarkConfiguration;
import icu.wwj.benchmark.tpcc.sharding.ShardingConfig;
import icu.wwj.benchmark.tpcc.sharding.ShardingNumber;
import io.vertx.core.Future;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.Tuple;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class NewOrderExecutor implements TransactionExecutor<Boolean> {
    
    private final BenchmarkConfiguration configuration;

    private final jTPCCRandom random;

    public PreparedQuery<RowSet<Row>> stmtNewOrderSelectWhseCust;

    public PreparedQuery<RowSet<Row>> stmtNewOrderSelectDist;

    public PreparedQuery<RowSet<Row>> stmtNewOrderUpdateDist;

    public PreparedQuery<RowSet<Row>> stmtNewOrderInsertOrder;

    public PreparedQuery<RowSet<Row>> stmtNewOrderInsertNewOrder;

    public PreparedQuery<RowSet<Row>> stmtNewOrderSelectStock;

    public PreparedQuery<RowSet<Row>> stmtNewOrderSelectItem;

    public PreparedQuery<RowSet<Row>> stmtNewOrderUpdateStock;

    public PreparedQuery<RowSet<Row>> stmtNewOrderInsertOrderLine;

    public NewOrderExecutor(BenchmarkConfiguration configuration, jTPCCRandom random, SqlConnection connection) {
        this.configuration = configuration;
        this.random = random;
        stmtNewOrderSelectWhseCust = connection.preparedQuery(
                "SELECT c_discount, c_last, c_credit, w_tax " +
                        "FROM bmsql_customer " +
                        "JOIN bmsql_warehouse ON (w_id = c_w_id) " +
                        "WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3");
        stmtNewOrderSelectDist = connection.preparedQuery(
                "SELECT d_tax, d_next_o_id " +
                        "FROM bmsql_district " +
                        "WHERE d_w_id = $1 AND d_id = $2 " +
                        "FOR UPDATE");
        stmtNewOrderUpdateDist = connection.preparedQuery(
                "UPDATE bmsql_district " +
                        "SET d_next_o_id = d_next_o_id + 1 " +
                        "WHERE d_w_id = $1 AND d_id = $2");
        stmtNewOrderInsertOrder = connection.preparedQuery(
                "INSERT INTO bmsql_oorder (" +
                        "    o_id, o_d_id, o_w_id, o_c_id, o_entry_d, " +
                        "    o_ol_cnt, o_all_local) " +
                        "VALUES ($1, $2, $3, $4, $5, $6, $7)");
        stmtNewOrderInsertNewOrder = connection.preparedQuery(
                "INSERT INTO bmsql_new_order (" +
                        "no_o_id, no_d_id, no_w_id) " +
                        "VALUES ($1, $2, $3)");
        stmtNewOrderSelectStock = connection.preparedQuery(
                "SELECT s_quantity, s_data, " +
                        "   s_dist_01, s_dist_02, s_dist_03, s_dist_04, " +
                        "   s_dist_05, s_dist_06, s_dist_07, s_dist_08, " +
                        "   s_dist_09, s_dist_10 " +
                        "FROM bmsql_stock " +
                        "WHERE s_w_id = $1 AND s_i_id = $2 " +
                        "FOR UPDATE");
        String selectItemSQL = ShardingConfig.instance.routeItemByHint
                ? "SELECT i_price, i_name, i_data FROM bmsql_item,bmsql_warehouse WHERE i_id = $1 AND w_id = $2"
                : "SELECT i_price, i_name, i_data FROM bmsql_item WHERE i_id = $1";
        stmtNewOrderSelectItem = connection.preparedQuery(selectItemSQL);
        stmtNewOrderUpdateStock = connection.preparedQuery(
                "UPDATE bmsql_stock " +
                        "SET s_quantity = $1, s_ytd = s_ytd + $2, " +
                        "    s_order_cnt = s_order_cnt + 1, " +
                        "    s_remote_cnt = s_remote_cnt + $3 " +
                        "WHERE s_w_id = $4 AND s_i_id = $5");
        stmtNewOrderInsertOrderLine = connection.preparedQuery(
                "INSERT INTO bmsql_order_line (" +
                        "    ol_o_id, ol_d_id, ol_w_id, ol_number, " +
                        "    ol_i_id, ol_supply_w_id, ol_quantity, " +
                        "    ol_amount, ol_dist_info) " +
                        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)");
    }

    private NewOrder generateNewOrder(int warehouse) {
        NewOrder newOrder = new NewOrder(warehouse, random.nextInt(1, 10), random.getCustomerID());
        int i = 0;
        // 2.4.1.3
        int o_ol_cnt = random.nextInt(5, 15);
        // 2.4.1.5
        for (; i < o_ol_cnt; i++) {
            newOrder.ol_i_id[i] = ShardingConfig.instance.routeItemByHint ? random.getItemID() : random.getItemIdByWarehouse(newOrder.w_id);
            if (random.nextInt(1, 100) <= 99) {
                newOrder.ol_supply_w_id[i] = warehouse;
            } else {
                int randomWarehouse = ShardingNumber.oneSharding(newOrder.w_id, random.nextInt(1, configuration.getWarehouses()));
                newOrder.ol_supply_w_id[i] = randomWarehouse;
                if (randomWarehouse != warehouse) {
                    newOrder.o_all_local = 0;
                }
            }
            newOrder.ol_quantity[i] = random.nextInt(1, 10);
        }
        // 2.4.1.4
        if (random.nextInt(1, 100) == 1) {
            newOrder.ol_i_id[i - 1] = ShardingNumber.oneSharding(newOrder.w_id, random.nextInt(1, 9) * 1000000 + newOrder.ol_i_id[i - 1]);
        }
        for (; i < 15; i++) {
            newOrder.ol_i_id[i] = 0;
            newOrder.ol_supply_w_id[i] = 0;
            newOrder.ol_quantity[i] = 0;
        }
        return newOrder;
    }

    @Override
    public Future<Boolean> execute(Transaction transaction, int warehouseId) {
        NewOrder generated = generateNewOrder(warehouseId);
        // The o_entry_d is now.
        generated.o_entry_d = LocalDateTime.now();
        int ol_cnt;
        final int[] ol_seq = new int[15];
        /*
         * When processing the order lines we must select the STOCK rows
         * FOR UPDATE. This is because we must perform business logic
         * (the juggling with the S_QUANTITY) here in the application
         * and cannot do that in an atomic UPDATE statement while getting
         * the original value back at the same time (UPDATE ... RETURNING
         * may not be vendor neutral). This can lead to possible deadlocks
         * if two transactions try to lock the same two stock rows in
         * opposite order. To avoid that we process the order lines in
         * the order of the order of ol_supply_w_id, ol_i_id.
         */
        for (ol_cnt = 0; ol_cnt < 15 && generated.ol_i_id[ol_cnt] != 0; ol_cnt++) {
            ol_seq[ol_cnt] = ol_cnt;
        }
        // TODO Consider refactoring this by Arrays.sort
        for (int x = 0; x < ol_cnt - 1; x++) {
            for (int y = x + 1; y < ol_cnt; y++) {
                if (generated.ol_supply_w_id[ol_seq[y]] < generated.ol_supply_w_id[ol_seq[x]]) {
                    int tmp = ol_seq[x];
                    ol_seq[x] = ol_seq[y];
                    ol_seq[y] = tmp;
                } else if (generated.ol_supply_w_id[ol_seq[y]] == generated.ol_supply_w_id[ol_seq[x]]
                        && generated.ol_i_id[ol_seq[y]] < generated.ol_i_id[ol_seq[x]]) {
                    int tmp = ol_seq[x];
                    ol_seq[x] = ol_seq[y];
                    ol_seq[y] = tmp;
                }
            }
        }
        generated.o_ol_cnt = ol_cnt;
        Future<NewOrder> future = Future.succeededFuture(generated);
        return future.compose(newOrder -> stmtNewOrderSelectDist.execute(Tuple.of(newOrder.w_id, newOrder.d_id))
                // Retrieve the required data from DISTRICT
                .map(rows -> {
                    if (0 == rows.size()) {
                        throw new IllegalStateException("District for W_ID=%d D_ID=%d not found".formatted(newOrder.w_id, newOrder.d_id));
                    }
                    Row row = rows.iterator().next();
                    newOrder.d_tax = row.getDouble("d_tax");
                    newOrder.o_id = row.getInteger("d_next_o_id");
                    return newOrder;
                })
        ).compose(newOrder -> stmtNewOrderSelectWhseCust.execute(Tuple.of(newOrder.w_id, newOrder.d_id, newOrder.c_id))
                // Retrieve the required data from CUSTOMER and WAREHOUSE
                .map(rows -> {
                    if (0 == rows.size()) {
                        throw new IllegalStateException("Warehouse or Customer for W_ID=%d D_ID=%d C_ID=%d not found".formatted(newOrder.w_id, newOrder.d_id, newOrder.c_id));
                    }
                    Row row = rows.iterator().next();
                    newOrder.w_tax = row.getDouble("w_tax");
                    newOrder.c_last = row.getString("c_last");
                    newOrder.c_credit = row.getString("c_credit");
                    newOrder.c_discount = row.getDouble("c_discount");
                    return newOrder;
                })
        ).compose(newOrder -> stmtNewOrderUpdateDist.execute(Tuple.of(newOrder.w_id, newOrder.d_id)).map(newOrder)
                // Update the DISTRICT bumping the D_NEXT_O_ID
        ).compose(newOrder -> stmtNewOrderInsertOrder.execute(Tuple.of(newOrder.o_id, newOrder.d_id, newOrder.w_id, newOrder.c_id, LocalDateTime.now(), newOrder.o_ol_cnt, newOrder.o_all_local)).map(newOrder)
                // Insert the ORDER row
        ).compose(newOrder -> stmtNewOrderInsertNewOrder.execute(Tuple.of(newOrder.o_id, newOrder.d_id, newOrder.w_id)).map(newOrder)
                // Insert the NEW_ORDER row
        ).compose(newOrder -> {
            List<Tuple> orderLineInserts = new ArrayList<>(newOrder.o_ol_cnt);
            List<Tuple> stockUpdates = new ArrayList<>(newOrder.o_ol_cnt);
            Future<Void> orderLineFuture = Future.succeededFuture();
            for (int i = 0; i < newOrder.o_ol_cnt; i++) {
                int ol_number = i + 1;
                int seq = ol_seq[i];
                final int i_id = newOrder.ol_i_id[seq];
                orderLineFuture = orderLineFuture.compose(__ -> processItem(newOrder, ol_number, i_id, seq, orderLineInserts, stockUpdates));
            }
            return orderLineFuture
                    .eventually(__ -> stockUpdates.isEmpty() ? Future.succeededFuture() : stmtNewOrderUpdateStock.executeBatch(stockUpdates))
                    .eventually(__ -> orderLineInserts.isEmpty() ? Future.succeededFuture() : stmtNewOrderInsertOrderLine.executeBatch(orderLineInserts))
                    .compose(__ -> transaction.commit().map(false),
                            cause -> transaction.rollback()
                                    .compose(unused -> ItemInvalidException.INSTANCE == cause ? Future.succeededFuture(true) : Future.failedFuture(cause)));
        });
    }

    private Future<Void> processItem(NewOrder newOrder, int ol_number, int i_id, int seq, List<Tuple> orderLineInserts, List<Tuple> stockUpdates) {
        return stmtNewOrderSelectItem.execute(ShardingConfig.instance.routeItemByHint ? Tuple.of(i_id, newOrder.w_id) : Tuple.of(i_id)).compose(itemRows -> {
            if (0 == itemRows.size()) {
                if (i_id < 1 || i_id > 100000) {
                    return Future.failedFuture(ItemInvalidException.INSTANCE);
                }
            }
            Row itemRow = itemRows.iterator().next();
            newOrder.i_name[seq] = itemRow.getString("i_name");
            newOrder.i_price[seq] = itemRow.getDouble("i_price");
            String i_data = itemRow.getString("i_data");
            return stmtNewOrderSelectStock.execute(Tuple.of(newOrder.ol_supply_w_id[seq], newOrder.ol_i_id[seq])).map(stockRows -> {
                if (0 == stockRows.size()) {
                    throw new IllegalStateException("STOCK with S_W_ID=%d S_I_ID=%d not fount".formatted(newOrder.ol_supply_w_id[seq], newOrder.ol_i_id[seq]));
                }
                Row stockRow = stockRows.iterator().next();
                newOrder.s_quantity[seq] = stockRow.getInteger("s_quantity");
                newOrder.ol_amount[seq] = newOrder.i_price[seq] * newOrder.ol_quantity[seq];
                newOrder.brand_generic[seq] = i_data.contains("ORIGINAL") && stockRow.getString("s_data").contains("ORIGINAL") ? "B" : "G";
                newOrder.total_amount += newOrder.ol_amount[seq] * (1.0 - newOrder.c_discount) * (1.0 + newOrder.w_tax + newOrder.d_tax);
                stockUpdates.add(Tuple.of(
                        newOrder.s_quantity[seq] + (newOrder.s_quantity[seq] >= newOrder.ol_quantity[seq] + 10 ? -newOrder.ol_quantity[seq] : +91),
                        newOrder.ol_quantity[seq],
                        newOrder.ol_supply_w_id[seq] == newOrder.w_id ? 0 : 1,
                        newOrder.ol_supply_w_id[seq],
                        newOrder.ol_i_id[seq]));
                orderLineInserts.add(Tuple.of(
                        newOrder.o_id,
                        newOrder.d_id,
                        newOrder.w_id,
                        ol_number,
                        newOrder.ol_i_id[seq],
                        newOrder.ol_supply_w_id[seq],
                        newOrder.ol_quantity[seq],
                        newOrder.ol_amount[seq],
                        stockRow.getString(newOrder.d_id < 10 ? "s_dist_0" + newOrder.d_id : "s_dist_10")
                ));
                return null;
            });
        });
    }

    private static class ItemInvalidException extends Exception {

        public static final ItemInvalidException INSTANCE = new ItemInvalidException();
    }
}
