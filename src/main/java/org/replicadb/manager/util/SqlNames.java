package org.replicadb.manager.util;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;

@Log4j2
public class SqlNames {

    /**
     * @param options
     * @param rsmd
     * @return
     * @throws SQLException
     */
    public static String getAllSinkColumns(ToolOptions options, ResultSetMetaData rsmd) throws SQLException {

        if (options.getSinkColumns() != null && !options.getSinkColumns().isEmpty()) {
            return options.getSinkColumns();
        } else if (options.getSourceColumns() != null && !options.getSourceColumns().isEmpty()) {
            return options.getSourceColumns();
        } else {
            options.setSinkColumns(getColumnsFromResultSetMetaData(options, rsmd));
            log.warn("Options source-columns and sink-columns are null, getting from Source ResultSetMetaData: " + options.getSinkColumns());
            return options.getSinkColumns();
        }
    }

    /**
     * @param options
     * @param rsmd
     * @return
     * @throws SQLException
     */
    private static String getColumnsFromResultSetMetaData(ToolOptions options, ResultSetMetaData rsmd) throws SQLException {

        StringBuilder columnNames = new StringBuilder();

        int columnsNumber = rsmd.getColumnCount();

        for (int i = 1; i <= columnsNumber; i++) {
            if (i > 1) columnNames.append(",");
            if (options.getQuotedIdentifiers())
                columnNames.append("\"").append(rsmd.getColumnName(i)).append("\"");
            else
                columnNames.append(rsmd.getColumnName(i));
        }
        return columnNames.toString();
    }


}
