package icu.wwj.benchmark.tpcc;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResultFileWriter {

    public static final String ADDRESS = ResultFileWriter.class.getSimpleName();

    private static final Logger LOGGER = LoggerFactory.getLogger(ResultFileWriter.class);

    private final AsyncFile file;

    private final MessageConsumer<String> resultConsumer;

    public ResultFileWriter(Vertx vertx, String path) {
        file = vertx.fileSystem().openBlocking(path, new OpenOptions());
        file.write(Buffer.buffer("run,elapsed,latency,dblatency,ttype,rbk,dskipped,error\n"));
        resultConsumer = vertx.eventBus().localConsumer(ADDRESS, this::handleResult);
    }

    private void handleResult(Message<String> result) {
        file.write(Buffer.buffer(result.body() + '\n'));
    }

    public Future<Void> close() {
        // TODO Risk: This may discard results which pending in queue.
        return resultConsumer.unregister()
                .compose(__ -> file.close())
                .onSuccess(__ -> LOGGER.info("Result file writer closed."))
                .onFailure(cause -> LOGGER.error("Failed to close result file writer", cause));
    }
}
