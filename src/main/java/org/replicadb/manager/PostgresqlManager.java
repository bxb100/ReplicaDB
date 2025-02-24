package org.replicadb.manager;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.jdbc.PgConnection;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;

@Log4j2
public class PostgresqlManager extends SqlManager {

    private final static char[] hexArray = "0123456789ABCDEF".toCharArray();

    private static Long chunkSize = 0L;

    /**
     * Constructs the SqlManager.
     *
     * @param opts the ReplicaDB ToolOptions describing the user's requested action.
     */
    public PostgresqlManager(ToolOptions opts, DataSourceType dsType) {
        super(opts);
        this.dsType = dsType;
    }

    @Override
    public String getDriverClass() {
        return JdbcDrivers.POSTGRES.getDriverClass();
    }

    @Override
    public int insertDataToTable(ResultSet resultSet, int taskId) throws SQLException, IOException {

        CopyIn copyIn = null;
        int totalRows = 0;

        try {

            ResultSetMetaData rsmd = resultSet.getMetaData();
            String tableName;

            // Get table name and columns
            if (options.getMode().equals(ReplicationMode.COMPLETE.getModeText())) {
                tableName = getSinkTableName();
            } else {
                tableName = getQualifiedStagingTableName();
            }

            String allColumns = getAllSinkColumns(rsmd);

            // Get Postgres COPY meta-command manager
            PgConnection copyOperationConnection = this.connection.unwrap(PgConnection.class);
            CopyManager copyManager = new CopyManager(copyOperationConnection);
            String copyCmd = getCopyCommand(tableName, allColumns);
            copyIn = copyManager.copyIn(copyCmd);

            char unitSeparator = 0x1F;
            char nullAscii = 0x00;
            int columnsNumber = rsmd.getColumnCount();

            StringBuilder row = new StringBuilder();
            StringBuilder cols = new StringBuilder();

            byte[] bytes;
            String colValue;

            if (resultSet.next()) {
                // Create Bandwidth Throttling
                bandwidthThrottlingCreate(resultSet, rsmd);

                do {
                    bandwidthThrottlingAcquiere();

                    // Get Columns values
                    for (int i = 1; i <= columnsNumber; i++) {
                        if (i > 1) cols.append(unitSeparator);

                        switch (rsmd.getColumnType(i)) {

                            case Types.CLOB:
                                colValue = clobToString(resultSet.getClob(i));
                                break;
                            //case Types.BINARY:
                            case Types.BLOB:
                                colValue = blobToPostgresHex(resultSet.getBlob(i));
                                break;
                            default:
                                colValue = resultSet.getString(i);
                                if (colValue == null)
                                    colValue = String.valueOf(nullAscii);
                                break;
                        }

                        if (!resultSet.wasNull() || colValue != null)
                            cols.append(colValue);
                    }

                    // Escape special chars
                    if (this.options.getSinkDisableEscape())
                        row.append(cols.toString().replace("\u0000", "\\N"));
                    else
                        row.append(cols.toString().replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r").replace("\u0000", "\\N"));

                    // Row ends with \n
                    row.append("\n");

                    // Copy data to postgres
                    bytes = row.toString().getBytes(StandardCharsets.UTF_8);
                    copyIn.writeToCopy(bytes, 0, bytes.length);

                    // Clear StringBuilders
                    row.setLength(0); // set length of buffer to 0
                    row.trimToSize();
                    cols.setLength(0); // set length of buffer to 0
                    cols.trimToSize();
                    totalRows++;
                } while (resultSet.next());
            }

            copyIn.endCopy();

        } catch (Exception e) {
            if (copyIn != null && copyIn.isActive()) {
                copyIn.cancelCopy();
            }
            this.connection.rollback();
            throw e;
        } finally {
            if (copyIn != null && copyIn.isActive()) {
                copyIn.cancelCopy();
            }
        }

        this.getConnection().commit();

        return totalRows;
    }

    private String getCopyCommand(String tableName, String allColumns) {

        StringBuilder copyCmd = new StringBuilder();

        copyCmd.append("COPY ");
        copyCmd.append(tableName);

        if (allColumns != null) {
            copyCmd.append(" (");
            copyCmd.append(allColumns);
            copyCmd.append(")");
        }

        copyCmd.append(" FROM STDIN WITH DELIMITER e'\\x1f' ENCODING 'UTF-8' ");

        log.info("Copying data with this command: " + copyCmd);

        return copyCmd.toString();
    }


    @Override
    public ResultSet readTable(String tableName, String[] columns, int nThread) throws SQLException {

        // If table name parameter is null get it from options
        tableName = tableName == null ? this.options.getSourceTable() : tableName;

        // If columns parameter is null, get it from options
        String allColumns = this.options.getSourceColumns() == null ? "*" : this.options.getSourceColumns();

        long offset = nThread * chunkSize;
        String sqlCmd;

        // Read table with source-query option specified
        if (options.getSourceQuery() != null && !options.getSourceQuery().isEmpty()) {
            sqlCmd = "SELECT  * FROM (" +
                    options.getSourceQuery() + ") as T1 OFFSET ? ";
        } else {

            sqlCmd = "SELECT " +
                    allColumns +
                    " FROM " +
                    escapeTableName(tableName);

            // Source Where
            if (options.getSourceWhere() != null && !options.getSourceWhere().isEmpty()) {
                sqlCmd = sqlCmd + " WHERE " + options.getSourceWhere();
            }

            sqlCmd = sqlCmd + " OFFSET ? ";

        }

        String limit = " LIMIT ?";

        if (this.options.getJobs() == nThread + 1) {
            return super.execute(sqlCmd, offset);
        } else {
            sqlCmd = sqlCmd + limit;
            return super.execute(sqlCmd, offset, chunkSize);
        }

    }

    @Override
    protected void createStagingTable() throws SQLException {

        Statement statement = this.getConnection().createStatement();
        try {

            String sinkStagingTable = getQualifiedStagingTableName();

            String sql = "CREATE UNLOGGED TABLE IF NOT EXISTS " + sinkStagingTable + " ( LIKE " + this.getSinkTableName() + " INCLUDING DEFAULTS INCLUDING CONSTRAINTS ) WITH (autovacuum_enabled=false)";

            log.info("Creating staging table with this command: " + sql);
            statement.executeUpdate(sql);
            statement.close();
            this.getConnection().commit();

        } catch (Exception e) {
            statement.close();
            this.connection.rollback();
            throw e;
        }

    }

    @Override
    protected void mergeStagingTable() throws SQLException {

        Statement statement = this.getConnection().createStatement();

        try {
            String[] pks = this.getSinkPrimaryKeys(this.getSinkTableName());
            // Primary key is required
            if (pks == null || pks.length == 0) {
                throw new IllegalArgumentException("Sink table must have at least one primary key column for incremental mode.");
            }

            // options.sinkColumns was set during the insertDataToTable
            String allColls = getAllSinkColumns(null);

            StringBuilder sql = new StringBuilder();
            sql.append("INSERT INTO ")
                    .append(this.getSinkTableName())
                    .append(" (")
                    .append(allColls)
                    .append(" ) ")
                    .append(" SELECT ")
                    .append(allColls)
                    .append(" FROM ")
                    .append(this.getSinkStagingTableName())
                    .append(" ON CONFLICT ")
                    .append(" (").append(String.join(",", pks)).append(" )")
                    .append(" DO UPDATE SET ");

            // Set all columns for DO UPDATE SET statement
            for (String colName : allColls.split(",")) {
                sql.append(" ").append(colName).append(" = excluded.").append(colName).append(" ,");
            }
            // Delete the last comma
            sql.setLength(sql.length() - 1);

            log.info("Merging staging table and sink table with this command: " + sql);
            statement.executeUpdate(sql.toString());
            statement.close();
            this.getConnection().commit();

        } catch (Exception e) {
            statement.close();
            this.connection.rollback();
            throw e;
        }
    }

    @Override
    public void preSourceTasks() throws SQLException {

        if (this.options.getJobs() != 1) {

            /**
             * Calculating the chunk size for parallel job processing
             */
            Statement statement = this.getConnection().createStatement();

            try {
                String sql = "SELECT " +
                        " abs(count(*) / " + options.getJobs() + ") chunk_size" +
                        ", count(*) total_rows" +
                        " FROM ";

                // Source Query
                if (options.getSourceQuery() != null && !options.getSourceQuery().isEmpty()) {
                    sql = sql + "( " + this.options.getSourceQuery() + " ) as T1";

                } else {

                    sql = sql + this.options.getSourceTable();
                    // Source Where
                    if (options.getSourceWhere() != null && !options.getSourceWhere().isEmpty()) {
                        sql = sql + " WHERE " + options.getSourceWhere();
                    }
                }

                log.debug("Calculating the chunks size with this sql: " + sql);
                ResultSet rs = statement.executeQuery(sql);
                rs.next();
                chunkSize = rs.getLong(1);
                long totalNumberRows = rs.getLong(2);
                log.debug("chunkSize: " + chunkSize + " totalNumberRows: " + totalNumberRows);

                statement.close();
                this.getConnection().commit();
            } catch (Exception e) {
                statement.close();
                this.connection.rollback();
                throw e;
            }
        }

    }

    @Override
    public void postSourceTasks() {/*Not implemented*/}

    /*********************************************************************************************
     * From BLOB to Hexadecimal String for Postgres Copy
     * @return string representation of blob
     *********************************************************************************************/
    private String blobToPostgresHex(Blob blobData) throws SQLException {

        String returnData = "";

        if (blobData != null) {
            try {
                byte[] bytes = blobData.getBytes(1, (int) blobData.length());

                char[] hexChars = new char[bytes.length * 2];
                for (int j = 0; j < bytes.length; j++) {
                    int v = bytes[j] & 0xFF;
                    hexChars[j * 2] = hexArray[v >>> 4];
                    hexChars[j * 2 + 1] = hexArray[v & 0x0F];
                }
                returnData = "\\\\x" + new String(hexChars);
            } finally {
                // The most important thing here is free the BLOB to avoid memory Leaks
                blobData.free();
            }
        }

        return returnData;

    }


}
