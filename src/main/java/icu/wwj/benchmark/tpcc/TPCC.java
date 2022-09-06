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

public final class TPCC {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(TPCC.class);
    
    private final BenchmarkConfiguration configuration;
    
    private final Vertx vertx;
    
    private final Pool pool;
    
    private final ResultReporter resultReporter;
    
    private final ResultFileWriter resultFileWriter;
    
    private long sessionStartNanoTime;
    
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
        sessionStartNanoTime = System.nanoTime();
        vertx.eventBus().publish("start", sessionStartNanoTime);
        Promise<Void> promise = Promise.promise();
        vertx.setTimer(TimeUnit.SECONDS.toMillis(configuration.getRunSeconds()), event -> {
            LOGGER.info("Stopping terminals.");
            vertx.undeploy(deploymentId).onSuccess(__ -> promise.complete()).onFailure(cause -> LOGGER.error("Error occurred:", cause));
        });
        if (configuration.getReportIntervalSeconds() > 0) {
            vertx.setPeriodic(configuration.getReportIntervalSeconds() * 1000L, __ -> reportCurrentTPM());
        }
        return promise.future()
                .onSuccess(compositeFuture -> LOGGER.info("Total: {}", resultReporter.sumTotalCount()))
                .onSuccess(compositeFuture -> LOGGER.info("New Order: {}", resultReporter.sumNewOrderCount()));
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
        start.onSuccess(__ -> LOGGER.info("TPC-C Finished")).eventually(__ -> vertx.close());
    }

    private void reportCurrentTPM() {
        long currentNanoTime = System.nanoTime();
        long elapsedNanoTime = (currentNanoTime - sessionStartNanoTime) / 1000000;
        double tpmC = (double) (100 * 60 * 1000 * resultReporter.sumNewOrderCount() / elapsedNanoTime) / 100.0;
        double tpmTotal = (double) (100 * 60 * 1000 * resultReporter.sumTotalCount() / elapsedNanoTime) / 100.0;
        LOGGER.info("Current tpmTOTAL: {}\tCurrent tpmC: {}", tpmTotal, tpmC);
    }
}
