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
        log.info("Terminal-{} started.", id);
        sendNextTransaction();
    }

    private void handleTPCCTransaction(Message<String> message) {
        if (stop) {
            stopPromise.complete();
            return;
        }
        long transactionStartNanoTime = System.nanoTime();
        (switch (message.body()) {
            case "NEW_ORDER" -> connection.begin().compose(newOrderExecutor::execute).onSuccess(__ -> newOrderCount.incrementAndGet());
            case "PAYMENT" -> connection.begin().compose(paymentExecutor::execute).map(false);
            case "ORDER_STATUS" -> connection.begin().compose(orderStatusExecutor::execute).map(false);
            case "STOCK_LEVEL" -> connection.begin().compose(stockLevelExecutor::execute).map(false);
            // TODO Transaction delivery which is required to execute in background is executed immediately at present
            case "DELIVERY" -> connection.begin().compose(deliveryExecutor::execute).map(false);
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
        // TODO complete transactions
        getVertx().eventBus().send(address, switch (random.nextInt(0, 10)) {
            case 1, 2, 3, 4 -> TPCCTransaction.NEW_ORDER.name();
            case 5, 6, 7, 8 -> TPCCTransaction.PAYMENT.name();
            case 9 -> TPCCTransaction.ORDER_STATUS.name();
            case 10 -> TPCCTransaction.STOCK_LEVEL.name();
            default -> TPCCTransaction.DELIVERY.name();
        });
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        this.stopPromise = stopPromise;
        stop = true;
    }
}
