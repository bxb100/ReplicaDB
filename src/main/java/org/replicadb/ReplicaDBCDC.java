package org.replicadb;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.connect.source.SourceRecord;
import org.replicadb.manager.ConnManager;

@Log4j2
public class ReplicaDBCDC implements Runnable {

    public final ConnManager sourceDs;
    public final ConnManager sinkDs;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private DebeziumEngine<?> engine;

    public ReplicaDBCDC(ConnManager sourceDs, ConnManager sinkDs) {
        this.sourceDs = sourceDs;
        this.sinkDs = sinkDs;
    }

    @Override
    public void run() {
        Properties debeziumProps = sourceDs.getDebeziumProps();

        // Create the engine with this configuration ...
        engine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(debeziumProps)
                .notifying((DebeziumEngine.ChangeConsumer<RecordChangeEvent<SourceRecord>>) sinkDs)
                .using(new EngineCompletionCallBack())
                .build();

        // Run the engine asynchronously ...
        executor.execute(engine);

        log.info("Engine executor started");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Requesting embedded engine to shut down");
            try {
                engine.close();
                awaitTermination(executor);
                log.info("Embedded engine is down");
            } catch (Exception e) {
                log.error("Error stopping Embedded engine: {}", e.getMessage());
                log.error("Salgo por aqui");
                log.error(e);
            }
        }));

        // the submitted task keeps running, only no more new ones can be added
        executor.shutdown();
        awaitTermination(executor);

    }

    private void awaitTermination(ExecutorService executor) {
        try {
            while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.info("Waiting another 10 seconds for the embedded engine to complete");

               /* try {
                    // TODO if streaming es false me piro
                    log.info("Me piro");
                    engine.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    log.error("Salgo por aqui");
                }
                executor.shutdown();

                */

            }
        } catch (Exception e) {
            log.error("Salgo por aqui");
            Thread.currentThread().interrupt();
        }
    }

    public static class EngineCompletionCallBack implements DebeziumEngine.CompletionCallback {
        @Override
        public void handle(boolean success, String message, Throwable error) {
            log.info("por aqui: {} ", success);
            //log.error("por aqui: {} ",message);
            log.error(error);

            if (!success && message.contains("Unable to initialize")) {
                // rearrancar el engine TODO
                log.error("Rearrancar la aplicación aqui! no se como...");
            }

        }
    }
}
