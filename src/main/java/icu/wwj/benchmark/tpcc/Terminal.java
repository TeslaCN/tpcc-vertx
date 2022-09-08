package icu.wwj.benchmark.tpcc;

import icu.wwj.benchmark.tpcc.config.BenchmarkConfiguration;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlConnection;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class Terminal extends AbstractVerticle {
    
    private final BenchmarkConfiguration configuration;

    private final int id;

    private final jTPCCRandom random;

    private final Pool pool;

    private SqlConnection connection;
    
    private final String[] transactionsMap;
    
    private final String address;

    private volatile long sessionStartNanoTime;

    private MessageConsumer<String> transactionConsumer;

    private MessageProducer<String> resultProducer;

    private NewOrderExecutor newOrderExecutor;

    private PaymentExecutor paymentExecutor;

    private OrderStatusExecutor orderStatusExecutor;

    private StockLevelExecutor stockLevelExecutor;

    private DeliveryExecutor deliveryExecutor;

    private final AtomicLong newOrderCount;

    private final AtomicLong totalCount;
    
    private int warehouseId;

    private boolean stop;

    private Promise<Void> stopPromise;

    public Terminal(BenchmarkConfiguration configuration, int id, Pool pool, ResultReporter resultReporter) {
        this.configuration = configuration;
        this.id = id;
        random = new jTPCCRandom();
        this.pool = pool;
        address = "Terminal-" + id;
        newOrderCount = resultReporter.getNewOrderCounts()[id - 1];
        totalCount = resultReporter.getTotalCount()[id - 1];
        transactionsMap = TransactionMapGenerator.generate(configuration);
    }

    @Override
    public void start(Promise<Void> startPromise) {
        pool.getConnection().onSuccess(this::init).onSuccess(__ -> startPromise.complete()).onFailure(cause -> log.error(cause.getMessage(), cause));
    }
    
    private void init(SqlConnection sqlConnection) {
        connection = sqlConnection;
        newOrderExecutor = new NewOrderExecutor(configuration, random, connection);
        paymentExecutor = new PaymentExecutor(configuration, random, connection);
        orderStatusExecutor = new OrderStatusExecutor(configuration, random, connection);
        stockLevelExecutor = new StockLevelExecutor(configuration, random, connection);
        deliveryExecutor = new DeliveryExecutor(configuration, random, connection);
        transactionConsumer = getVertx().eventBus().localConsumer(address);
        resultProducer = getVertx().eventBus().sender(ResultFileWriter.ADDRESS);
        MessageConsumer<Long> startConsumer = getVertx().eventBus().localConsumer("start");
        startConsumer.handler(msg -> {
            startExecutingTransactions(msg.body());
            startConsumer.unregister();
        });
    }

    public void startExecutingTransactions(long sessionStartNanoTime) {
        transactionConsumer.handler(this::handleTPCCTransaction);
        this.sessionStartNanoTime = sessionStartNanoTime;
        if (configuration.isTerminalWarehouseFixed()) {
            warehouseId = random.nextInt(1, configuration.getWarehouses());
            log.info("Terminal-{} started. w_id = {}", id, warehouseId);
        } else {
            log.info("Terminal-{} started. w_id is not fixed", id);
        }
        sendNextTransaction();
    }

    private void handleTPCCTransaction(Message<String> message) {
        if (stop) {
            connection.close().eventually(__ -> {
                stopPromise.complete();
                return Future.succeededFuture();
            });
            return;
        }
        if (!configuration.isTerminalWarehouseFixed()) {
            warehouseId = random.nextInt(1, configuration.getWarehouses());
        }
        long transactionStartNanoTime = System.nanoTime();
        (switch (message.body()) {
            case "NEW_ORDER" -> connection.begin().compose(transaction -> newOrderExecutor.execute(transaction, warehouseId)).onSuccess(__ -> newOrderCount.incrementAndGet());
            case "PAYMENT" -> connection.begin().compose(transaction -> paymentExecutor.execute(transaction, warehouseId)).map(false);
            case "ORDER_STATUS" -> connection.begin().compose(transaction -> orderStatusExecutor.execute(transaction, warehouseId)).map(false);
            case "STOCK_LEVEL" -> connection.begin().compose(transaction -> stockLevelExecutor.execute(transaction, warehouseId)).map(false);
            // TODO Transaction delivery which is required to execute in background is executed immediately at present
            case "DELIVERY" -> connection.begin().compose(transaction -> deliveryExecutor.execute(transaction, warehouseId)).map(false);
            default -> Future.failedFuture("Unknown transaction type");
        }).onSuccess(rollback -> onTransactionSuccess(transactionStartNanoTime, message.body(), (boolean) rollback))
                .recover(cause -> {
                    log.error("Error occurred running " + message.body(), cause);
                    return Future.succeededFuture();
                })
                .onSuccess(__ -> sendNextTransaction());
    }

    private void onTransactionSuccess(long transactionStartNanoTime, String transactionType, boolean rollback) {
        totalCount.incrementAndGet();
        long transactionFinishNanoTime = System.nanoTime();
        long elapsedMillis = (transactionFinishNanoTime - sessionStartNanoTime) / 1_000_000;
        long transactionTookMillis = (transactionFinishNanoTime - transactionStartNanoTime) / 1_000_000;
        resultProducer.write("0," + elapsedMillis + "," + transactionTookMillis + "," + transactionTookMillis + "," + transactionType + "," + (rollback ? "1" : "0") + ",0,0");
    }

    private void sendNextTransaction() {
        getVertx().eventBus().send(address, transactionsMap[(random.nextInt(0, transactionsMap.length - 1))]);
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        this.stopPromise = stopPromise;
        stop = true;
    }
}
