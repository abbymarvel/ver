package ddprofiler.sources.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import ddprofiler.sources.SourceType;

public class ClickHouseSourceConfig implements SourceConfig {

    private String sourceName;

    private String relationName;

    @JsonProperty
    private String databaseName;

    @JsonProperty
    private String dbServerIp;

    @JsonProperty
    private int dbServerPort = 8123; // Default ClickHouse HTTP port

    @JsonProperty
    private String dbUsername;

    @JsonProperty
    private String dbPassword;

    @JsonProperty
    private String protocol = "http"; // Default protocol

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getDbServerIp() {
        return dbServerIp;
    }

    public void setDbServerIp(String dbServerIp) {
        this.dbServerIp = dbServerIp;
    }

    public int getDbServerPort() {
        return dbServerPort;
    }

    public void setDbServerPort(int dbServerPort) {
        this.dbServerPort = dbServerPort;
    }

    public String getDbUsername() {
        return dbUsername;
    }

    public void setDbUsername(String dbUsername) {
        this.dbUsername = dbUsername;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public void setDbPassword(String dbPassword) {
        this.dbPassword = dbPassword;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.clickhouse;
    }

    @Override
    public String getSourceName() {
        return sourceName;
    }

    @Override
    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public String getRelationName() {
        return relationName;
    }

    @Deprecated
    public void setRelationName(String relationName) {
        this.relationName = relationName;
    }

    @Override
    public String getPath() {
        StringBuilder connectionString = new StringBuilder(protocol + "://");
        connectionString.append(dbServerIp).append(":").append(dbServerPort);
        
        return connectionString.toString();
    }

    @Override
    public SourceConfig selfCopy() {
        ClickHouseSourceConfig copy = new ClickHouseSourceConfig();
        copy.sourceName = this.sourceName;
        copy.relationName = this.relationName;
        copy.databaseName = this.databaseName;
        copy.dbServerIp = this.dbServerIp;
        copy.dbServerPort = this.dbServerPort;
        copy.dbUsername = this.dbUsername;
        copy.dbPassword = this.dbPassword;
        copy.protocol = this.protocol;
        return copy;
    }
}
