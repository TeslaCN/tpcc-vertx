package icu.wwj.benchmark.tpcc;

import io.vertx.core.Future;
import io.vertx.sqlclient.Transaction;

public interface TransactionExecutor<T> {
    
    Future<T> execute(Transaction transaction, int warehouseId);
}
