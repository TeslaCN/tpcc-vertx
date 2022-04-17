package icu.wwj.benchmark.tpcc;

import icu.wwj.benchmark.tpcc.config.Configurations;
import io.vertx.core.Future;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.Tuple;

public class StockLevelExecutor implements TransactionExecutor<Void> {

    private final jTPCCRandom random;

    private final PreparedQuery<RowSet<Row>> stmtStockLevelSelectLow;

    public StockLevelExecutor(jTPCCRandom random, SqlConnection connection) {
        this.random = random;
        stmtStockLevelSelectLow = connection.preparedQuery(
                "SELECT count(*) AS low_stock FROM (" +
                        "    SELECT s_w_id, s_i_id, s_quantity " +
                        "        FROM bmsql_stock " +
                        "        WHERE s_w_id = $1 AND s_quantity < $2 AND s_i_id IN (" +
                        "            SELECT ol_i_id " +
                        "                FROM bmsql_district " +
                        "                JOIN bmsql_order_line ON ol_w_id = d_w_id " +
                        "                 AND ol_d_id = d_id " +
                        "                 AND ol_o_id >= d_next_o_id - 20 " +
                        "                 AND ol_o_id < d_next_o_id " +
                        "                WHERE d_w_id = $1 AND d_id = $3 " +
                        "        ) " +
                        "    ) AS L");
    }

    @Override
    public Future<Void> execute(Transaction transaction) {
        StockLevel stockLevel = generateStockLevel();
        return stmtStockLevelSelectLow.execute(Tuple.of(stockLevel.w_id, stockLevel.threshold, stockLevel.d_id)).compose(rows -> {
            if (0 == rows.size()) {
                throw new IllegalStateException("Failed to get low-stock for W_ID=%d D_ID=%d".formatted(stockLevel.w_id, stockLevel.d_id));
            }
            stockLevel.low_stock = rows.iterator().next().getInteger("low_stock");
            return transaction.rollback();
        });
    }

    private StockLevel generateStockLevel() {
        return new StockLevel(random.nextInt(1, Configurations.WAREHOUSES), random.nextInt(1, 10), random.nextInt(10, 20));
    }
}
