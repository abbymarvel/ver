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
        StringBuilder connectionString = new StringBuilder("clickhouse://");
        if (dbUsername != null && !dbUsername.isEmpty()) {
            connectionString.append(dbUsername);
            if (dbPassword != null && !dbPassword.isEmpty()) {
                connectionString.append(":").append(dbPassword);
            }
            connectionString.append("@");
        }

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
        return copy;
    }
}
