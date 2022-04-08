package icu.wwj.benchmark.tpcc;

import icu.wwj.benchmark.tpcc.config.Configurations;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class TPCC {

    private static final Logger LOGGER = LoggerFactory.getLogger(TPCC.class);

    private final Vertx vertx = Vertx.vertx(new VertxOptions()
            .setPreferNativeTransport(true)
            .setEventLoopPoolSize(Runtime.getRuntime().availableProcessors())
            // We use the worker pool only when writing result to file.
            .setWorkerPoolSize(1)
    );

    private final Pool pool;

    private final ResultRecorder resultRecorder;

    private final Terminal[] terminals = new Terminal[Configurations.TERMINALS];

    public TPCC() {
        vertx.exceptionHandler(cause -> LOGGER.error("Unhandled exception", cause));
        pool = PgPool.pool(vertx, new PgConnectOptions().setCachePreparedStatements(true)
                .setHost("127.0.0.1")
                .setDatabase("bmsql")
                .setUser("postgres")
                .setPassword("postgres"), new PoolOptions().setMaxSize(32));
        resultRecorder = new ResultRecorder(vertx, "/tmp/tpcc_result_" + DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now()) + ".csv");
    }

    @SuppressWarnings("rawtypes")
    public Future<Void> run() {
        LOGGER.info("Starting TPC-C.");
        List<Future> futures = new ArrayList<>(terminals.length);
        for (int i = 0; i < terminals.length; i++) {
            int finalI = i;
            futures.add(pool.getConnection().compose(connection -> {
                Terminal terminal = new Terminal(finalI, vertx.eventBus(), connection);
                terminals[finalI] = terminal;
                return Future.succeededFuture(terminal);
            }));
        }
        return CompositeFuture.all(futures)
                .compose(this::onTerminalsReady)
                .onFailure(cause -> LOGGER.error("Failed to start terminals, caused by:", cause))
                .eventually(__ -> resultRecorder.close())
                .eventually(__ -> vertx.close());
    }

    @SuppressWarnings("rawtypes")
    public Future<Void> onTerminalsReady(CompositeFuture succeeded) {
        LOGGER.info("Starting terminals.");
        List<Terminal> terminals = succeeded.list();
        long sessionStartNanoTime = System.nanoTime();
        List<Future> futures = terminals.stream().map(terminal -> terminal.run(sessionStartNanoTime)).collect(Collectors.toList());
        LOGGER.info("All terminals started.");
        vertx.setTimer(TimeUnit.SECONDS.toMillis(Configurations.SECONDS), event -> {
            LOGGER.info("Stopping terminals.");
            for (Terminal each : terminals) {
                each.stop();
            }
        });
        return CompositeFuture.all(futures)
                .onSuccess(compositeFuture -> LOGGER.info("Total: {}", compositeFuture.<Terminal>list().stream().mapToInt(Terminal::getTotalCount).sum()))
                .onSuccess(compositeFuture -> LOGGER.info("New Order: {}", compositeFuture.<Terminal>list().stream().mapToInt(Terminal::getNewOrderCount).sum()))
                .onFailure(cause -> LOGGER.error("Error occurred:", cause))
                .compose(__ -> Future.succeededFuture());
    }

    public static void main(String[] args) {
        Future<Void> start = new TPCC().run();
        start.onSuccess(__ -> LOGGER.info("Succeeded"));
    }
}
