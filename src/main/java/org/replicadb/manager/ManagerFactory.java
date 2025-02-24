package org.replicadb.manager;

import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.cdc.OracleManagerCDC;
import org.replicadb.manager.cdc.SQLServerManagerCDC;
import static org.replicadb.manager.SupportedManagers.DENODO;
import static org.replicadb.manager.SupportedManagers.FILE;
import static org.replicadb.manager.SupportedManagers.KAFKA;
import static org.replicadb.manager.SupportedManagers.MARIADB;
import static org.replicadb.manager.SupportedManagers.MYSQL;
import static org.replicadb.manager.SupportedManagers.ORACLE;
import static org.replicadb.manager.SupportedManagers.POSTGRES;
import static org.replicadb.manager.SupportedManagers.S3;
import static org.replicadb.manager.SupportedManagers.SQLSERVER;

/**
 * Contains instantiation code for all ConnManager implementations
 * ManagerFactories are instantiated by o.a.h.s.ConnFactory and
 * stored in an ordered list. The ConnFactory.getManager() implementation
 * calls the accept() method of each ManagerFactory, in order until
 * one such call returns a non-null ConnManager instance.
 */
@Log4j2
public class ManagerFactory {

    /**
     * Instantiate a ConnManager that can fulfill the database connection
     * requirements of the task specified in the JobData.
     *
     * @param options the user-provided arguments that configure this
     *                Sqoop job.
     * @return a ConnManager that can connect to the specified database
     * and perform the operations required, or null if this factory cannot
     * find a suitable ConnManager implementation.
     */
    public ConnManager accept(ToolOptions options, DataSourceType dsType) {

        String scheme = extractScheme(options, dsType);

        if (null == scheme) {
            // We don't know if this is a mysql://, hsql://, etc.
            // Can't do anything with this.
            log.warn("Null scheme associated with connect string.");
            return null;
        }

        log.trace("Trying with scheme: " + scheme);

        if (options.getMode().equals(ReplicationMode.CDC.getModeText())) {
            log.debug("CDC Managers");

            if (SQLSERVER.isTheManagerTypeOf(options, dsType)) {
                return new SQLServerManagerCDC(options, dsType);
            } else if (ORACLE.isTheManagerTypeOf(options, dsType)) {
                return new OracleManagerCDC(options, dsType);
            } else {
                throw new IllegalArgumentException("The database with scheme " + scheme + " is not supported in CDC mode");
            }

        } else {
            if (POSTGRES.isTheManagerTypeOf(options, dsType)) {
                return new PostgresqlManager(options, dsType);
            } else if (ORACLE.isTheManagerTypeOf(options, dsType)) {
                return new OracleManager(options, dsType);
            } else if (DENODO.isTheManagerTypeOf(options, dsType)) {
                return new DenodoManager(options, dsType);
//            } else if (CSV.isTheManagerTypeOf(options, dsType)) {
//                return new CsvManager(options, dsType);
            } else if (KAFKA.isTheManagerTypeOf(options, dsType)) {
                return new KafkaManager(options, dsType);
            } else if (SQLSERVER.isTheManagerTypeOf(options, dsType)) {
                return new SQLServerManager(options, dsType);
            } else if (S3.isTheManagerTypeOf(options, dsType)) {
                return new S3Manager(options, dsType);
            } else if (MYSQL.isTheManagerTypeOf(options, dsType) || MARIADB.isTheManagerTypeOf(options, dsType)) {
                return new MySQLManager(options, dsType);
            } else if (FILE.isTheManagerTypeOf(options, dsType)) {
                return new LocalFileManager(options, dsType);
            } else {
                throw new IllegalArgumentException("The database with scheme " + scheme + " is not supported by ReplicaDB");
            }
        }

    }

    protected String extractScheme(ToolOptions options, DataSourceType dsType) {
        return SupportedManagers.extractScheme(options, dsType);
    }

}
