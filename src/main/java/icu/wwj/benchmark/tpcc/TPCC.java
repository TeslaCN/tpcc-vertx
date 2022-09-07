package icu.wwj.benchmark.tpcc;

import icu.wwj.benchmark.tpcc.config.BenchmarkConfiguration;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;
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
import java.util.concurrent.atomic.AtomicReference;

public final class TPCC {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(TPCC.class);
    
    private final BenchmarkConfiguration configuration;
    
    private final Vertx vertx;
    
    private final Pool pool;
    
    private final ResultReporter resultReporter;
    
    private final ResultFileWriter resultFileWriter;
    
    private LocalDateTime sessionStartLocalDateTime;
    
    private long sessionStartNanoTime;
    
    private LocalDateTime sessionStopLocalDateTime;
    
    private long sessionStopNanoTime;
    
    public TPCC(BenchmarkConfiguration configuration, Vertx vertx, Pool pool) {
        this.configuration = configuration;
        this.vertx = vertx;
        vertx.exceptionHandler(cause -> LOGGER.error("Unhandled exception", cause));
        this.pool = pool;
        resultReporter = new ResultReporter(configuration.getTerminals());
        resultFileWriter = new ResultFileWriter(vertx, "/tmp/tpcc_result_" + DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now()) + ".csv");
    }
    
    public Future<Void> run() {
        LOGGER.info("Starting TPC-C.");
        AtomicInteger idGenerator = new AtomicInteger();
        return vertx.deployVerticle(() -> new Terminal(configuration, idGenerator.incrementAndGet(), pool, resultReporter), new DeploymentOptions().setInstances(configuration.getTerminals()))
                .compose(this::onTerminalsReady)
                .onFailure(cause -> LOGGER.error("Failed to start terminals, caused by:", cause))
                .eventually(__ -> resultFileWriter.close());
    }

    public Future<Void> onTerminalsReady(String deploymentId) {
        LOGGER.info("Starting terminals.");
        sessionStartLocalDateTime = LocalDateTime.now();
        sessionStartNanoTime = System.nanoTime();
        vertx.eventBus().publish("start", sessionStartNanoTime);
        Promise<Void> promise = Promise.promise();
        AtomicReference<Long> realtimeReporter = new AtomicReference<>();
        if (configuration.getReportIntervalSeconds() > 0) {
            realtimeReporter.set(vertx.setPeriodic(configuration.getReportIntervalSeconds() * 1000L, __ -> reportCurrentTPM()));
        }
        vertx.setTimer(TimeUnit.SECONDS.toMillis(configuration.getRunSeconds()), event -> {
            LOGGER.info("Stopping terminals.");
            if (null != realtimeReporter.get()) {
                vertx.cancelTimer(realtimeReporter.get());
            }
            vertx.undeploy(deploymentId).onSuccess(__ -> {
                sessionStopNanoTime = System.nanoTime();
                sessionStopLocalDateTime = LocalDateTime.now();
                promise.complete();
            }).onFailure(cause -> LOGGER.error("Error occurred:", cause));
        });
        return promise.future().onSuccess(__ -> finalReport());
    }
    
    private void reportCurrentTPM() {
        long currentNanoTime = System.nanoTime();
        long elapsedNanoTime = (currentNanoTime - sessionStartNanoTime) / 1000000;
        double tpmC = (double) (100 * 60 * 1000 * resultReporter.sumNewOrderCount() / elapsedNanoTime) / 100.0;
        double tpmTotal = (double) (100 * 60 * 1000 * resultReporter.sumTotalCount() / elapsedNanoTime) / 100.0;
        LOGGER.info("Current tpmTOTAL: {}\tCurrent tpmC: {}", tpmTotal, tpmC);
    }
    
    private void finalReport() {
        long elapsedNanoTime = (sessionStopNanoTime - sessionStartNanoTime) / 1000000;
        long newOrderCount = resultReporter.sumNewOrderCount();
        long totalCount = resultReporter.sumTotalCount();
        double tpmC = (double) (100 * 60 * 1000 * newOrderCount / elapsedNanoTime) / 100.0;
        double tpmTotal = (double) (100 * 60 * 1000 * totalCount / elapsedNanoTime) / 100.0;
        LOGGER.info("Measured tpmC (NewOrders) = {}", tpmC);
        LOGGER.info("Measured tpmTOTAL = {}", tpmTotal);
        LOGGER.info("Session Start     = {}", sessionStartLocalDateTime);
        LOGGER.info("Session End       = {}", sessionStopLocalDateTime);
        LOGGER.info("Transaction Count = {}", totalCount);
        LOGGER.info("New Order Count   = {}", newOrderCount);
    }
    
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.load(new FileInputStream(Paths.get(System.getProperty("props", "props.template")).toFile()));
        BenchmarkConfiguration configuration = new BenchmarkConfiguration(props);
        Vertx vertx = Vertx.vertx(new VertxOptions(new JsonObject(configuration.getVertxOptions())));
        // cachePreparedStatements could not be specified in URI. https://github.com/eclipse-vertx/vertx-sql-client/issues/664
        SqlConnectOptions connectOptions = SqlConnectOptions.fromUri(configuration.getConn()).setCachePreparedStatements(true);
        Pool pool = Pool.pool(vertx, connectOptions, new PoolOptions(new JsonObject(configuration.getPoolOptions())));
        Future<Void> start = new TPCC(configuration, vertx, pool).run();
        start.onSuccess(__ -> LOGGER.info("TPC-C Finished")).eventually(__ -> pool.close()).eventually(__ -> vertx.close());
    }
}
