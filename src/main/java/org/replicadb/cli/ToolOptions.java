package org.replicadb.cli;

import java.io.IOException;
import java.util.Properties;

import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

@Data
@Log4j2
public class ToolOptions {

    private static final int DEFAULT_JOBS = 4;
    private static final int DEFAULT_FETCH_SIZE = 5000;
    private static final String DEFAULT_MODE = ReplicationMode.COMPLETE.getModeText();

    private String sourceConnect;
    private String sourceUser;
    private String sourcePassword;
    private String sourceTable;
    private String sourceColumns;
    private String sourceWhere;
    private String sourceQuery;
    private String sourceFileFormat;

    private String sinkConnect;
    private String sinkUser;
    private String sinkPassword;
    private String sinkTable;
    private String sinkStagingTable;
    private String sinkStagingTableAlias;
    private String sinkStagingSchema;
    private String sinkColumns;
    private String sinkFileformat;
    private Boolean sinkDisableEscape = false;
    private Boolean sinkDisableIndex = false;
    private Boolean sinkDisableTruncate = false;
    private Boolean sinkAnalyze = false;


    private int jobs = DEFAULT_JOBS;
    private int fetchSize = DEFAULT_FETCH_SIZE;
    private int bandwidthThrottling = 0;
    private boolean help = false;
    // FIXME: change name
    private boolean versionCheck = false;
    private boolean verbose = false;
    private Boolean quotedIdentifiers = false;
    private String optionsFile;

    private String mode = DEFAULT_MODE;

    private Properties sourceConnectionParams;
    private Properties sinkConnectionParams;
    private String sentryDsn;

    private Options options;

    public ToolOptions(String[] args) throws ParseException, IOException {
        checkOptions(args);
    }

    private void checkOptions(String[] args) throws ParseException, IOException {

        this.options = new Options();

        // Source Options
        options.addOption(
                Option.builder()
                        .longOpt("source-connect")
                        .desc("Source database JDBC connect string")
                        .hasArg()
                        //.required()
                        .argName("jdbc-uri")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("source-user")
                        .desc("Source database authentication username")
                        .hasArg()
                        .argName("username")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("source-password")
                        .desc("Source database authentication password")
                        .hasArg()
                        .argName("password")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("source-table")
                        .desc("Source database table to read")
                        .hasArg()
                        .argName("table-name")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("source-columns")
                        .desc("Source database table columns to be extracted")
                        .hasArg()
                        .argName("col,col,col...")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("source-where")
                        .desc("Source database WHERE clause to use during extraction")
                        .hasArg()
                        .argName("where clause")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("source-query")
                        .desc("SQL statement to be executed in the source database")
                        .hasArg()
                        .argName("statement")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("source-file-format")
                        .desc("Source file format. The allowed values are csv, json, avro, parquet, orc")
                        .hasArg()
                        .argName("file format")
                        .build()
        );

        // Sink Options
        options.addOption(
                Option.builder()
                        .longOpt("sink-connect")
                        .desc("Sink database JDBC connect string")
                        .hasArg()
                        //.required()
                        .argName("jdbc-uri")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("sink-user")
                        .desc("Sink database authentication username")
                        .hasArg()
                        .argName("username")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("sink-password")
                        .desc("Sink database authentication password")
                        .hasArg()
                        .argName("password")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("sink-table")
                        .desc("Sink database table to populate")
                        .hasArg()
                        .argName("table-name")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("sink-columns")
                        .desc("Sink database table columns to be populated")
                        .hasArg()
                        .argName("col,col,col...")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("sink-disable-escape")
                        .desc("Escape strings before populating to the table of the sink database.")
                        .build()
        );


        options.addOption(
                Option.builder()
                        .longOpt("sink-disable-index")
                        .desc("Disable sink database table indexes before populate.")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("sink-disable-truncate")
                        .desc("Disable the truncation of the sink database table before populate.")
                        .build()
        );


        options.addOption(
                Option.builder()
                        .longOpt("sink-analyze")
                        .desc("Analyze sink database table after populate.")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("sink-staging-table")
                        .desc("Qualified name of the sink staging table. The table must exist in the sink database.")
                        .hasArg()
                        .argName("staging-table-name")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("sink-staging-table-alias")
                        .desc("Alias name for the sink staging table. The table must exist in the sink database.")
                        .hasArg()
                        .argName("staging-table-name-alias")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("sink-staging-schema")
                        .desc("Scheme name on the sink database, with right permissions for creating staging tables.")
                        .hasArg()
                        .argName("staging-schema")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("sink-file-format")
                        .desc("Sink file format. The allowed values are csv, json, avro, parquet, orc")
                        .hasArg()
                        .argName("file format")
                        .build()
        );

        // Other Options
        options.addOption(
                Option.builder()
                        .longOpt("options-file")
                        .desc("Options file path location")
                        .hasArg()
                        .argName("file-path")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("mode")
                        .desc("Specifies the replication mode. The allowed values are complete, complete-atomic, incremental or cdc.")
                        //.required()
                        .hasArg()
                        .argName("mode")
                        .build()
        );


        options.addOption(
                Option.builder()
                        .longOpt("fetch-size")
                        .desc("Number of entries to read from database at once.")
                        .hasArg()
                        .argName("fetch-size")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("version")
                        .desc("Show implementation version and exit.")
                        .build()
        );

        options.addOption(
                Option.builder()
                        .longOpt("bandwidth-throttling")
                        .desc("Adds a bandwidth cap for the replication in KB/sec.")
                        .hasArg()
                        .argName("KB/s")
                        .build()
        );


        Option helpOpt = new Option("h", "help", false, "Print this help screen");
        options.addOption(helpOpt);

        Option jobsOpt = new Option("j", "jobs", true, "Use n jobs to replicate in parallel. Default 4");
        jobsOpt.setArgName("n");
        options.addOption(jobsOpt);

        Option verboseOpt = new Option("v", "verbose", false, "Print more information while working");
        options.addOption(verboseOpt);

        options.addOption(
                Option.builder()
                        .longOpt("quoted-identifiers")
                        .desc("Should all database identifiers be quoted.")
                        .build()
        );


        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        // If help argument is not passed is not necessary test the rest of arguments
        if (existsHelpArgument(args)) {
            printHelp();
            this.setHelp(true);
        } else if (existsVersionArgument(args)) {
            this.setVersionCheck(true);
        } else {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);

            // check for optionsFile
            setOptionsFile(line.getOptionValue("options-file"));
            if (this.optionsFile != null && !this.optionsFile.isEmpty()) {
                loadOptionsFile();
            }

            //get & set Options
            if (line.hasOption("verbose")) {
                enableVerbose();
            }
            if (line.hasOption("sink-disable-index")) {
                enableSinkDisableIndex();
            }
            if (line.hasOption("sink-disable-escape")) {
                enableSinkDisableEscape();
            }
            if (line.hasOption("sink-disable-truncate")) {
                enableSinkDisableTruncate();
            }
            if (line.hasOption("sink-analyze")) {
                enableSinkAnalyze();
            }
            if (line.hasOption("quoted-identifiers")) {
                enableQuotedIdentifiers();
            }

            setModeNotNull(line.getOptionValue("mode"));
            setSinkColumnsNotNull(line.getOptionValue("sink-columns"));
            setSinkConnectNotNull(line.getOptionValue("sink-connect"));
            setHelp(line.hasOption("help"));
            setSinkPasswordNotNull(line.getOptionValue("sink-password"));
            setSinkTableNotNull(line.getOptionValue("sink-table"));
            setSinkUserNotNull(line.getOptionValue("sink-user"));
            setSourceColumnsNotNull(line.getOptionValue("source-columns"));
            setSourceConnectNotNull(line.getOptionValue("source-connect"));
            setSourcePasswordNotNull(line.getOptionValue("source-password"));
            setSourceQueryNotNull(line.getOptionValue("source-query"));
            setSourceTableNotNull(line.getOptionValue("source-table"));
            setSourceUserNotNull(line.getOptionValue("source-user"));
            setSourceWhereNotNull(line.getOptionValue("source-where"));
            setJobsNotNull(line.getOptionValue("jobs"));
            setFetchSizeNotNull(line.getOptionValue("fetch-size"));
            setBandwidthThrottlingNotNull(line.getOptionValue("bandwidth-throttling"));
            setSinkStagingSchemaNotNull(line.getOptionValue("sink-staging-schema"));
            setSinkStagingTableNotNull(line.getOptionValue("sink-staging-table"));
            setSinkStagingTableAliasNotNull(line.getOptionValue("sink-staging-table-alias"));
            setSourceFileFormatNotNull(line.getOptionValue("source-file-format"));
            setSinkFileFormatNotNull(line.getOptionValue("sink-file-format"));

            //Check for required values
            if (!checkRequiredValues())
                throw new IllegalArgumentException("Missing any of the required parameters:" +
                        " source-connect=" + this.sourceConnect + " OR sink-connect=" + this.sinkConnect);
        }

    }


    private void printHelp() {
        String header = "\nArguments: \n";
        String footer = "\nPlease report issues at https://github.com/osalvador/ReplicaDB/issues";

        // automatically generate the help statement
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(140);
        formatter.printHelp("replicadb [OPTIONS]", header, this.options, footer, false);
    }

    private boolean existsHelpArgument(String[] args) {
        //help argument is -h or --help
        for (int i = 0; i <= args.length - 1; i++) {
            if (args[i].equals("-h") || args[i].equals("--help")) {
                return true;
            }
        }
        return false;
    }

    private boolean existsVersionArgument(String[] args) {
        //help argument is -h or --help
        for (int i = 0; i <= args.length - 1; i++) {
            if (args[i].equals("--version")) {
                return true;
            }
        }
        return false;
    }

    public String getVersion() {
        return ToolOptions.class.getPackage().getImplementationVersion();
    }

    public boolean checkRequiredValues() {

        if (this.mode == null) return false;
        if (this.sourceConnect == null) return false;
        return this.sinkConnect != null;
    }

    private void loadOptionsFile() throws IOException {

        OptionsFile of = new OptionsFile(this.optionsFile);

        // set properties from options file to this ToolOptions
        Properties prop = of.getProperties();
        setSinkAnalyze(Boolean.parseBoolean(prop.getProperty("sink.analyze")));

        setVerbose(Boolean.parseBoolean(prop.getProperty("verbose")));
        setMode(prop.getProperty("mode"));

        setSinkColumns(prop.getProperty("sink.columns"));
        setSinkConnect(prop.getProperty("sink.connect"));
        setSinkDisableIndex(Boolean.parseBoolean(prop.getProperty("sink.disable.index")));
        setSinkDisableEscape(Boolean.parseBoolean(prop.getProperty("sink.disable.escape")));
        setSinkDisableTruncate(Boolean.parseBoolean(prop.getProperty("sink.disable.truncate")));
        setSinkUser(prop.getProperty("sink.user"));
        setSinkPassword(prop.getProperty("sink.password"));
        setSinkTable(prop.getProperty("sink.table"));
        setSinkStagingTable(prop.getProperty("sink.staging.table"));
        setSinkStagingTableAlias(prop.getProperty("sink.staging.table.alias"));
        setSinkStagingSchema(prop.getProperty("sink.staging.schema"));
        setSourceColumns(prop.getProperty("source.columns"));
        setSourceConnect(prop.getProperty("source.connect"));
        setSourcePassword(prop.getProperty("source.password"));
        setSourceQuery(prop.getProperty("source.query"));
        setSourceTable(prop.getProperty("source.table"));
        setSourceUser(prop.getProperty("source.user"));
        setSourceWhere(prop.getProperty("source.where"));
        setJobs(prop.getProperty("jobs"));
        setFetchSize(prop.getProperty("fetch.size"));
        setBandwidthThrottling(prop.getProperty("bandwidth.throttling"));
        setQuotedIdentifiers(Boolean.parseBoolean(prop.getProperty("quoted.identifiers")));
        setSourceFileFormat(prop.getProperty("source.file.format"));
        setSinkFileformat(prop.getProperty("sink.file.format"));
        setSentryDsn(prop.getProperty("sentry.dsn"));

        // Connection params
        setSinkConnectionParams(of.getSinkConnectionParams());
        setSourceConnectionParams(of.getSourceConnectionParams());
    }

    /*
     * Getters & Setters
     */
    private void setSourceConnectNotNull(String sourceConnect) {
        if (sourceConnect != null && !sourceConnect.isEmpty())
            this.sourceConnect = sourceConnect;
    }

    public void setSourceUserNotNull(String sourceUser) {
        if (sourceUser != null && !sourceUser.isEmpty())
            this.sourceUser = sourceUser;
    }

    public void setSourcePasswordNotNull(String sourcePassword) {
        if (sourcePassword != null && !sourcePassword.isEmpty())
            this.sourcePassword = sourcePassword;
    }

    public void setSourceTableNotNull(String sourceTable) {
        if (sourceTable != null && !sourceTable.isEmpty())
            this.sourceTable = sourceTable;
    }

    public void setSourceColumnsNotNull(String sourceColumns) {
        if (sourceColumns != null && !sourceColumns.isEmpty())
            this.sourceColumns = sourceColumns;
    }

    public void setSourceWhereNotNull(String sourceWhere) {
        if (sourceWhere != null && !sourceWhere.isEmpty())
            this.sourceWhere = sourceWhere;
    }

    public void setSourceQueryNotNull(String sourceQuery) {
        if (sourceQuery != null && !sourceQuery.isEmpty())
            this.sourceQuery = sourceQuery;
    }

    public void setSinkConnectNotNull(String sinkConnect) {
        if (sinkConnect != null && !sinkConnect.isEmpty())
            this.sinkConnect = sinkConnect;
    }

    public void setSinkUserNotNull(String sinkUser) {
        if (sinkUser != null && !sinkUser.isEmpty())
            this.sinkUser = sinkUser;
    }

    public void setSinkPasswordNotNull(String sinkPassword) {
        if (sinkPassword != null && !sinkPassword.isEmpty())
            this.sinkPassword = sinkPassword;
    }

    public void setSinkTableNotNull(String sinkTable) {
        if (sinkTable != null && !sinkTable.isEmpty())
            this.sinkTable = sinkTable;
    }

    public void setSinkColumnsNotNull(String sinkColumns) {
        if (sinkColumns != null && !sinkColumns.isEmpty())
            this.sinkColumns = sinkColumns;
    }

    public void setJobs(String jobs) {
        try {
            if (jobs != null && !jobs.isEmpty()) {
                this.jobs = Integer.parseInt(jobs);
                if (this.jobs <= 0) throw new NumberFormatException();
            }
        } catch (NumberFormatException | NullPointerException e) {
            log.error("Option --jobs must be a positive integer grater than 0.");
            throw e;
        }
    }

    public void setJobsNotNull(String jobs) {
        if (jobs != null && !jobs.isEmpty())
            setJobs(jobs);
    }

    public void setMode(String mode) {

        if (mode != null && !mode.isEmpty()) {
            if (!mode.toLowerCase().equals(ReplicationMode.COMPLETE.getModeText())
                    && !mode.toLowerCase().equals(ReplicationMode.INCREMENTAL.getModeText())
                    && !mode.toLowerCase().equals(ReplicationMode.COMPLETE_ATOMIC.getModeText())
                    && !mode.toLowerCase().equals(ReplicationMode.CDC.getModeText())
            )
                throw new IllegalArgumentException("mode option must be "
                        + ReplicationMode.COMPLETE.getModeText()
                        + ", "
                        + ReplicationMode.COMPLETE_ATOMIC.getModeText()
                        + ", "
                        + ReplicationMode.INCREMENTAL.getModeText()
                        + " or "
                        + ReplicationMode.CDC.getModeText()
                );
        } else {
            // Default mode
            mode = ReplicationMode.COMPLETE.getModeText();
        }
        this.mode = mode.toLowerCase();
    }

    public void setModeNotNull(String mode) {
        if (mode != null && !mode.isEmpty())
            setMode(mode);
    }

    public void enableVerbose() {
        this.verbose = true;
    }

    public void enableSinkDisableIndex() {
        this.sinkDisableIndex = true;
    }

    public void enableSinkDisableEscape() {
        this.sinkDisableEscape = true;
    }

    private void enableSinkDisableTruncate() {
        this.sinkDisableTruncate = true;
    }

    public void enableSinkAnalyze() {
        this.sinkAnalyze = true;
    }

    public void enableQuotedIdentifiers() {
        this.quotedIdentifiers = true;
    }

    public void setSinkStagingTableNotNull(String sinkStagingTable) {
        if (sinkStagingTable != null)
            this.sinkStagingTable = sinkStagingTable;
    }

    public void setSinkStagingTableAliasNotNull(String sinkStagingTableAlias) {
        if (sinkStagingTableAlias != null)
            this.sinkStagingTableAlias = sinkStagingTableAlias;
    }


    public void setSinkStagingSchemaNotNull(String sinkStagingSchema) {
        if (sinkStagingSchema != null)
            this.sinkStagingSchema = sinkStagingSchema;
    }

    public void setFetchSize(String fetchSize) {
        try {
            if (fetchSize != null && !fetchSize.isEmpty()) {
                this.fetchSize = Integer.parseInt(fetchSize);
                if (this.fetchSize <= 0) throw new NumberFormatException();
            }
        } catch (NumberFormatException | NullPointerException e) {
            log.error("Option --fetch-size must be a positive integer grater than 0.");
            throw e;
        }

    }

    public void setFetchSizeNotNull(String fetchSize) {
        if (fetchSize != null && !fetchSize.isEmpty())
            setFetchSize(fetchSize);
    }

    public void setBandwidthThrottling(String bandwidthThrottling) {
        try {
            if (bandwidthThrottling != null && !bandwidthThrottling.isEmpty()) {
                this.bandwidthThrottling = Integer.parseInt(bandwidthThrottling);
                if (this.bandwidthThrottling < 0)
                    throw new NumberFormatException();
            }
        } catch (NumberFormatException | NullPointerException e) {
            log.error("Option --bandwidth-throttling must be a positive integer grater than 0.");
            throw e;
        }
    }

    public void setBandwidthThrottlingNotNull(String bandwidthThrottling) {
        if (bandwidthThrottling != null && !bandwidthThrottling.isEmpty())
            setBandwidthThrottling(bandwidthThrottling);
    }

    private void setSourceFileFormatNotNull(String fileFormat) {
        if (fileFormat != null && !fileFormat.isEmpty())
            this.sourceFileFormat = fileFormat;
    }

    private void setSinkFileFormatNotNull(String fileFormat) {
        if (fileFormat != null && !fileFormat.isEmpty())
            this.sinkFileformat = fileFormat;
    }
}
