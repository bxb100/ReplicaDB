package org.replicadb.file;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.file.FileFormats;
import org.replicadb.utils.ScriptRunner;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@Log4j2
class Csv2PostgresTest {
    private static final String RESOURECE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final String POSTGRES_SINK_FILE = "/sinks/pg-sink.sql";
    private static final String USER_PASSWD_DB = "replicadb";
    private static final int EXPECTED_ROWS = 1024;
    private static final String CSV_SOURCE_FILE = "/csv/source.csv";
    private static final String SOURCE_COLUMNS="C_VARCHAR,C_CHAR,C_LONGVARCHAR,C_INTEGER,C_BIGINT,C_TINYINT,C_SMALLINT,C_NUMERIC,C_DECIMAL,C_DOUBLE,C_FLOAT,C_DATE,C_TIMESTAMP,C_TIME,C_BOOLEAN";
    private static final String SINK_COLUMNS="C_CHARACTER_VAR,C_CHARACTER,C_CHARACTER_LOB,C_INTEGER,C_BIGINT,C_SMALLINT,C_REAL,C_NUMERIC,C_DECIMAL,C_DOUBLE_PRECISION,C_FLOAT,C_DATE,C_TIMESTAMP_WITHOUT_TIMEZONE,C_TIME_WITHOUT_TIMEZONE,C_BOOLEAN";


    private Connection postgresConn;

    @ClassRule
    public static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:9.6")
            .withDatabaseName(USER_PASSWD_DB)
            .withUsername(USER_PASSWD_DB)
            .withPassword(USER_PASSWD_DB);

    @BeforeAll
    static void setUp() throws SQLException, IOException {
        // Start the mariadb container
        postgres.start();
        // Create tables
        /*Postgres*/
        Connection con = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        ScriptRunner runner = new ScriptRunner(con, false, true);
        runner.runScript(new BufferedReader(new FileReader(RESOURECE_DIR + POSTGRES_SINK_FILE)));
        log.info("Creating Postgres sink tables");
        con.close();
    }

    @BeforeEach
    void before() throws SQLException {
        this.postgresConn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Truncate sink table and close connections
        postgresConn.createStatement().execute("TRUNCATE TABLE t_sink");
        this.postgresConn.close();
    }

    public int countSinkRows() throws SQLException {
        Statement stmt = postgresConn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t_sink");
        rs.next();
        int count = rs.getInt(1);
        log.info("Total rows in the sink table: {}", count);
        return count;
    }

    @Test
    void testPostgresConnection() throws SQLException {
        Statement stmt = postgresConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1");
        rs.next();
        String version = rs.getString(1);
        log.info(version);
        assertTrue(version.contains("1"));
    }

    @Test
    void testCsv2PostgresComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURECE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", "file://" + RESOURECE_DIR + CSV_SOURCE_FILE,
                "--source-file-format", FileFormats.CSV.getType(),
                "--source-columns",SOURCE_COLUMNS,
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--sink-columns", SINK_COLUMNS
        };
        ToolOptions options = new ToolOptions(args);
        Properties sourceConnectionParams = new Properties();
        sourceConnectionParams.setProperty("columns.types", "VARCHAR, CHAR, LONGVARCHAR, INTEGER, BIGINT, TINYINT, SMALLINT, NUMERIC, DECIMAL, DOUBLE, FLOAT, DATE, TIMESTAMP, TIME, BOOLEAN");
        sourceConnectionParams.setProperty("format.firstRecordAsHeader", "true");
        options.setSourceConnectionParams(sourceConnectionParams);

        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

}