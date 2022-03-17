package org.replicadb.cli;

import java.io.FileReader;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Set;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class OptionsFile {

    private static final String SOURCE_CONNECTION_PREFIX = "source.connect.parameter.";
    private static final String SINK_CONNECTION_PREFIX = "sink.connect.parameter.";

    private final Properties properties;

    public OptionsFile(String optionsFilePath) throws IOException {
        this.properties = new Properties();
        loadProperties(optionsFilePath);
    }

    public Properties getProperties() {
        return properties;
    }

    private void loadProperties(String optionsFilePath) throws IOException {

        // open reader to read the properties file
        try (FileReader in = new FileReader(optionsFilePath)) {
            // load the properties from that reader
            this.properties.load(in);
            resolvePropertiesEnvVar();
        } catch (IOException e) {
            // handle the exception
            log.error(e);
            throw e;
        }
    }

    public Properties getSourceConnectionParams() {

        Set<Object> propertyKeys = this.properties.keySet();
        Properties sourceConnectProps = new Properties();
        String connectionProperty;
        String value;

        for (Object propertyKey : propertyKeys) {
            String key = (String) propertyKey;

            if (key.startsWith(SOURCE_CONNECTION_PREFIX)) {
                connectionProperty = key.substring(SOURCE_CONNECTION_PREFIX.length());
                value = this.properties.getProperty(key);
                sourceConnectProps.setProperty(connectionProperty, value);
            }
        }

        return sourceConnectProps;

    }

    public Properties getSinkConnectionParams() {
        Set<Object> propertyKeys = this.properties.keySet();
        Properties sinkConnectProps = new Properties();
        String connectionProperty;
        String value;

        for (Object propertyKey : propertyKeys) {
            String key = (String) propertyKey;

            if (key.startsWith(SINK_CONNECTION_PREFIX)) {
                connectionProperty = key.substring(SINK_CONNECTION_PREFIX.length());
                value = this.properties.getProperty(key);
                sinkConnectProps.setProperty(connectionProperty, value);
            }
        }

        return sinkConnectProps;
    }

    private void resolvePropertiesEnvVar() {
        Enumeration<?> propertyNames = this.properties.propertyNames();
        while (propertyNames.hasMoreElements()) {
            String name = propertyNames.nextElement().toString();
            String value = this.properties.getProperty(name);

            if (value != null && !value.isEmpty())
                this.properties.setProperty(name, EnvironmentVariableEvaluator.resolveEnvVars(value));

        }
    }
}
