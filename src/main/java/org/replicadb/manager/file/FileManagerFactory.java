package org.replicadb.manager.file;

import lombok.extern.log4j.Log4j2;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.DataSourceType;
import static org.replicadb.manager.file.FileFormats.CSV;
import static org.replicadb.manager.file.FileFormats.ORC;

@Log4j2
public class FileManagerFactory {

    /**
     * Instantiate a FileManager that can meet the requirements of the specified file format
     *
     * @param options the user-provided arguments
     * @param dsType  the type of the DataSource, source or sink.
     * @return
     */
    public FileManager accept(ToolOptions options, DataSourceType dsType) {
        String fileFormat = null;
        if (dsType == DataSourceType.SOURCE) {
            fileFormat = options.getSourceFileFormat();
        } else if (dsType == DataSourceType.SINK) {
            fileFormat = options.getSinkFileformat();
        } else {
            log.error("DataSourceType must be Source or Sink");
        }

        if (ORC.getType().equals(fileFormat)) {
            log.info("return OrcFileManager");
            return new OrcFileManager(options, dsType);
        } else if (CSV.getType().equals(fileFormat)) {
            log.info("return CsvFileManager");
            return new CsvFileManager(options, dsType);
        } else {
            // CSV is the Default file format
            log.warn("The file format is not defined, setting CSV as the default file format.");
            return new CsvFileManager(options, dsType);
        }
    }
}
