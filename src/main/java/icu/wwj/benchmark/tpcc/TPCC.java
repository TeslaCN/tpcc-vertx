package icu.wwj.benchmark.tpcc;

import icu.wwj.benchmark.tpcc.config.BenchmarkConfiguration;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class TPCC {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(TPCC.class);
    
    private final BenchmarkConfiguration configuration;
    
    private final Vertx vertx;
    
    private final Pool pool;
    
    private final ResultRecorder resultRecorder;
    
    public TPCC(BenchmarkConfiguration configuration, Vertx vertx, Pool pool) {
        this.configuration = configuration;
        this.vertx = vertx;
        vertx.exceptionHandler(cause -> LOGGER.error("Unhandled exception", cause));
        this.pool = pool;
        resultRecorder = new ResultRecorder(vertx, "/tmp/tpcc_result_" + DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now()) + ".csv");
    }
    
    public Future<Void> run() {
        LOGGER.info("Starting TPC-C.");
        AtomicInteger idGenerator = new AtomicInteger();
        return vertx.deployVerticle(() -> new Terminal(configuration, idGenerator.incrementAndGet(), pool), new DeploymentOptions().setInstances(configuration.getTerminals()))
                .compose(this::onTerminalsReady)
                .onFailure(cause -> LOGGER.error("Failed to start terminals, caused by:", cause))
                .eventually(__ -> resultRecorder.close());
    }

    public Future<Void> onTerminalsReady(String deploymentId) {
        LOGGER.info("Starting terminals.");
        long sessionStartNanoTime = System.nanoTime();
        vertx.eventBus().publish("start", sessionStartNanoTime);
        Promise<Void> promise = Promise.promise();
        AtomicLong newOrderCount = new AtomicLong(), totalCount = new AtomicLong();
        AtomicInteger remainTerminals = new AtomicInteger(configuration.getTerminals());
        vertx.setTimer(TimeUnit.SECONDS.toMillis(configuration.getRunSeconds()), event -> {
            LOGGER.info("Stopping terminals.");
            vertx.eventBus().<TerminalResult>localConsumer(TerminalResult.class.getSimpleName(), msg -> {
                newOrderCount.addAndGet(msg.body().getNewOrderCount());
                totalCount.addAndGet(msg.body().getTotalCount());
                if (0 == remainTerminals.decrementAndGet()) {
                    promise.complete();
                }
            });
            vertx.undeploy(deploymentId).onFailure(cause -> LOGGER.error("Error occurred:", cause));
        });
        return promise.future()
                .onSuccess(compositeFuture -> LOGGER.info("Total: {}", totalCount.get()))
                .onSuccess(compositeFuture -> LOGGER.info("New Order: {}", newOrderCount.get()));
    }
    
    public static void main(String[] args) throws IOException {
        Vertx vertx = Vertx.vertx(new VertxOptions()
                .setPreferNativeTransport(true)
                .setEventLoopPoolSize(Runtime.getRuntime().availableProcessors())
                // We use the worker pool only when writing result to file.
                .setWorkerPoolSize(1)
        );
        Properties props = new Properties();
        props.load(new FileInputStream(Paths.get(System.getProperty("props", "props.template")).toFile()));
        BenchmarkConfiguration configuration = new BenchmarkConfiguration(props);
        Pool pool = PgPool.pool(vertx, PgConnectOptions.fromUri(configuration.getConn()), new PoolOptions(new JsonObject(configuration.getPoolConfig())).setMaxWaitQueueSize(0));
        Future<Void> start = new TPCC(configuration, vertx, pool).run();
        start.onSuccess(__ -> LOGGER.info("TPC-C Finished")).eventually(__ -> vertx.close());
    }
}
