package icu.wwj.benchmark.tpcc;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.sqlclient.SqlConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Terminal {

    private final static Logger LOGGER = LoggerFactory.getLogger(Terminal.class);

    private final int id;

    private final jTPCCRandom random;

    private final EventBus eventBus;

    private final SqlConnection connection;

    private final String address;

    private final MessageConsumer<String> consumer;

    private volatile long sessionStartNanoTime;

    private volatile boolean stop;

    private final Promise<Terminal> stopPromise = Promise.promise();

    private int newOrderCount;

    private int totalCount;

    private final MessageProducer<String> resultProducer;

    private final NewOrderExecutor newOrderExecutor;

    private final PaymentExecutor paymentExecutor;

    private final OrderStatusExecutor orderStatusExecutor;

    private final StockLevelExecutor stockLevelExecutor;

    public Terminal(int id, EventBus eventBus, SqlConnection connection) {
        this.id = id;
        this.random = new jTPCCRandom();
        this.connection = connection;
        this.eventBus = eventBus;
        address = "Terminal-" + id;
        consumer = eventBus.localConsumer(address);
        resultProducer = eventBus.sender(ResultRecorder.ADDRESS);
        newOrderExecutor = new NewOrderExecutor(random, connection);
        paymentExecutor = new PaymentExecutor(random, connection);
        orderStatusExecutor = new OrderStatusExecutor(random, connection);
        stockLevelExecutor = new StockLevelExecutor(random, connection);
    }

    public Future<Terminal> run(long sessionStartNanoTime) {
        LOGGER.info("Terminal-{} started.", id);
        consumer.handler(this::handleTPCCTransaction);
        this.sessionStartNanoTime = sessionStartNanoTime;
        sendNextTransaction();
        return stopPromise.future();
    }

    private void handleTPCCTransaction(Message<String> message) {
        if (stop) {
            stopPromise.complete(this);
            return;
        }
        long transactionStartNanoTime = System.nanoTime();
        (switch (message.body()) {
            case "NEW_ORDER" -> connection.begin().compose(newOrderExecutor::execute).onSuccess(__ -> newOrderCount++);
            case "PAYMENT" -> connection.begin().compose(paymentExecutor::execute);
            case "ORDER_STATUS" -> connection.begin().compose(orderStatusExecutor::execute);
            case "STOCK_LEVEL" -> connection.begin().compose(stockLevelExecutor::execute);
            // TODO complete transactions
            case "DELIVERY" -> Future.failedFuture(new UnsupportedOperationException());
            default -> Future.failedFuture("Unknown transaction type");
        }).onSuccess(__ -> onTransactionSuccess(transactionStartNanoTime, message.body()))
                .recover(cause -> {
                    LOGGER.error("Error occurred running " + message.body(), cause);
                    return Future.succeededFuture();
                })
                .onSuccess(__ -> sendNextTransaction());
    }

    private void onTransactionSuccess(long transactionStartNanoTime, String transactionType) {
        totalCount++;
        long transactionFinishNanoTime = System.nanoTime();
        long elapsedMillis = (transactionFinishNanoTime - sessionStartNanoTime) / 1_000_000;
        long transactionTookMillis = (transactionFinishNanoTime - transactionStartNanoTime) / 1_000_000;
        resultProducer.write("0," + elapsedMillis + "," + transactionTookMillis + "," + transactionTookMillis + "," + transactionType + ",0,0,0");
    }

    private void sendNextTransaction() {
        // TODO complete transactions
        eventBus.send(address, switch (random.nextInt(1, 10)) {
            case 1, 2, 3, 4 -> TPCCTransaction.NEW_ORDER.name();
            case 5, 6, 7, 8 -> TPCCTransaction.PAYMENT.name();
            case 9 -> TPCCTransaction.ORDER_STATUS.name();
            default -> TPCCTransaction.STOCK_LEVEL.name();
        });
    }

    public void stop() {
        stop = true;
    }
    
    public int getNewOrderCount() {
        return newOrderCount;
    }

    public int getTotalCount() {
        return totalCount;
    }
}
