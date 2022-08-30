package icu.wwj.benchmark.tpcc;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlConnection;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Terminal extends AbstractVerticle {

    private final static Logger LOGGER = LoggerFactory.getLogger(Terminal.class);

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

    @Getter
    private long newOrderCount;

    @Getter
    private long totalCount;

    private boolean stop;

    private Promise<Void> stopPromise;

    public Terminal(int id, Pool pool) {
        this.id = id;
        random = new jTPCCRandom();
        this.pool = pool;
        address = "Terminal-" + id;
    }

    @Override
    public void start(Promise<Void> startPromise) {
        pool.getConnection().onSuccess(this::init).onSuccess(__ -> startPromise.complete());
    }
    
    private void init(SqlConnection sqlConnection) {
        connection = sqlConnection;
        newOrderExecutor = new NewOrderExecutor(random, connection);
        paymentExecutor = new PaymentExecutor(random, connection);
        orderStatusExecutor = new OrderStatusExecutor(random, connection);
        stockLevelExecutor = new StockLevelExecutor(random, connection);
        deliveryExecutor = new DeliveryExecutor(random, connection);
        transactionConsumer = getVertx().eventBus().localConsumer(address);
        resultProducer = getVertx().eventBus().sender(ResultRecorder.ADDRESS);
        MessageConsumer<Long> startConsumer = getVertx().eventBus().localConsumer("start");
        startConsumer.handler(msg -> {
            startExecutingTransactions(msg.body());
            startConsumer.unregister();
        });
    }

    public void startExecutingTransactions(long sessionStartNanoTime) {
        transactionConsumer.handler(this::handleTPCCTransaction);
        this.sessionStartNanoTime = sessionStartNanoTime;
        LOGGER.info("Terminal-{} started.", id);
        sendNextTransaction();
    }

    private void handleTPCCTransaction(Message<String> message) {
        if (stop) {
            getVertx().eventBus().publish(TerminalResult.class.getSimpleName(), new TerminalResult(totalCount, newOrderCount));
            stopPromise.complete();
            return;
        }
        long transactionStartNanoTime = System.nanoTime();
        (switch (message.body()) {
            case "NEW_ORDER" -> connection.begin().compose(newOrderExecutor::execute).onSuccess(__ -> newOrderCount++);
            case "PAYMENT" -> connection.begin().compose(paymentExecutor::execute).map(false);
            case "ORDER_STATUS" -> connection.begin().compose(orderStatusExecutor::execute).map(false);
            case "STOCK_LEVEL" -> connection.begin().compose(stockLevelExecutor::execute).map(false);
            // TODO Transaction delivery which is required to execute in background is executed immediately at present
            case "DELIVERY" -> connection.begin().compose(deliveryExecutor::execute).map(false);
            default -> Future.failedFuture("Unknown transaction type");
        }).onSuccess(rollback -> onTransactionSuccess(transactionStartNanoTime, message.body(), (boolean) rollback))
                .recover(cause -> {
                    LOGGER.error("Error occurred running " + message.body(), cause);
                    return Future.succeededFuture();
                })
                .onSuccess(__ -> sendNextTransaction());
    }

    private void onTransactionSuccess(long transactionStartNanoTime, String transactionType, boolean rollback) {
        totalCount++;
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
