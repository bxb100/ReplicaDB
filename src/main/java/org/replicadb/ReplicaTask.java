package org.replicadb;

import java.sql.ResultSet;
import java.util.concurrent.Callable;

import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.ConnManager;
import org.replicadb.manager.DataSourceType;
import org.replicadb.manager.ManagerFactory;

@Log4j2
final public class ReplicaTask implements Callable<Integer> {

    private final int taskId;
    private final ToolOptions options;
    private String taskName;


    public ReplicaTask(int id, ToolOptions options) {
        this.taskId = id;
        this.options = options;
    }

    @Override
    public Integer call() throws Exception {

        //System.out.println("Task ID :" + this.taskId + " performed by " + Thread.currentThread().getName());
        this.taskName = "TaskId-" + this.taskId;

        Thread.currentThread().setName(taskName);

        log.info("Starting " + Thread.currentThread().getName());

        // Do stuff...
        // Obtener una instancia del DriverManager del Source
        // Obtener una instancia del DriverManager del Sink
        // Mover datos de Source a Sink.
        ManagerFactory managerF = new ManagerFactory();
        ConnManager sourceDs = managerF.accept(options, DataSourceType.SOURCE);
        ConnManager sinkDs = managerF.accept(options, DataSourceType.SINK);


        try {
            sourceDs.getConnection();
        } catch (Exception e) {
            log.error("ERROR in " + this.taskName + " getting Source connection: " + e.getMessage());
            throw e;
        }

        try {
            sinkDs.getConnection();
        } catch (Exception e) {
            log.error("ERROR in " + this.taskName + " getting Sink connection: " + e.getMessage());
            throw e;
        }

        ResultSet rs;
        try {
            rs = sourceDs.readTable(null, null, taskId);
        } catch (Exception e) {
            log.error("ERROR in " + this.taskName + " reading source table: " + e.getMessage());
            throw e;
        }

        try {
            int processedRows = sinkDs.insertDataToTable(rs, taskId);
            // TODO determine the total rows processed in all the managers
            log.info("A total of {} rows processed by task {}", processedRows, taskId);
        } catch (Exception e) {
            log.error("ERROR in " + this.taskName + " inserting data to sink table: " + e.getMessage());
            throw e;
        }


        //ReplicaDB.printResultSet(rs);
        sourceDs.close();
        sinkDs.close();


        return this.taskId;
    }
}

