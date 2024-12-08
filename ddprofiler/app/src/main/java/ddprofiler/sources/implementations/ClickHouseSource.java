package ddprofiler.sources.implementations;

import static com.codahale.metrics.MetricRegistry.name;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import com.codahale.metrics.Counter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import metrics.Metrics;
import ddprofiler.sources.Source;
import ddprofiler.sources.SourceType;
import ddprofiler.sources.SourceUtils;
import ddprofiler.sources.config.ClickHouseSourceConfig;
import ddprofiler.sources.config.SourceConfig;
import ddprofiler.sources.deprecated.Attribute;
import ddprofiler.sources.deprecated.Record;

public class ClickHouseSource implements Source {

    final private Logger LOG = LoggerFactory.getLogger(ClickHouseSource.class.getName());

    private int tid;
    private String databaseName;
    private String relationName;
    private ClickHouseSourceConfig config;
    private Client client;
    private List<Attribute> attributes;

    private long lineCounter = 0;
    private Counter errorRecords = Metrics.REG.counter(name(ClickHouseSource.class, "error", "records"));
    private Counter successRecords = Metrics.REG.counter(name(ClickHouseSource.class, "success", "records"));

    public ClickHouseSource() {
    }

    public ClickHouseSource(String databaseName, String relationName, SourceConfig config) {
        this.tid = SourceUtils.computeTaskId(databaseName, relationName);
        this.databaseName = databaseName;
        this.relationName = relationName;
        this.config = (ClickHouseSourceConfig) config;
    }

    @Override
    public String getPath() {
        return config.getPath();
    }

    @Override
    public String getRelationName() {
        return relationName;
    }

    @Override
    public SourceConfig getSourceConfig() {
        return this.config;
    }

    @Override
    public int getTaskId() {
        return tid;
    }

    @Override
    public List<Source> processSource(SourceConfig config) {
        assert (config instanceof ClickHouseSourceConfig);
        this.config = (ClickHouseSourceConfig) config;

        List<Source> tasks = new ArrayList<>();
        ClickHouseSourceConfig clickHouseConfig = (ClickHouseSourceConfig) config;
        String connectionString = clickHouseConfig.getPath();
        LOG.info("Connecting to ClickHouse at: {}", connectionString);

        try (Client client = new Client.Builder().addEndpoint(connectionString)
                .setUsername(clickHouseConfig.getDbUsername()).setPassword(clickHouseConfig.getDbPassword())
                .setDefaultDatabase(clickHouseConfig.getDatabaseName()).build()) {
            
            // Read whole result set and process it record by record
            client.queryAll("SHOW TABLES").forEach(row -> {
                String tableName = row.getString(1);
                ClickHouseSource task = new ClickHouseSource(clickHouseConfig.getDatabaseName(), tableName, config);
                tasks.add(task);
            });
            
            LOG.info("Total tables processed: {}", tasks.size());
        } catch (Exception e) {
            LOG.error("Error connecting to ClickHouse: ", e);
        }
        return tasks;
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.clickhouse;
    }

    @Override
    public List<Attribute> getAttributes() {
        if (client == null) {
            connectToDatabase();
            attributes = new ArrayList<>();
            String query = String.format("DESCRIBE TABLE %s", this.relationName);
            try (QueryResponse response = client.query(query).get()) {
                ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
                attributes.add(new Attribute(reader.getString(1)));
                while (reader.hasNext()) {
                    reader.next();
                    attributes.add(new Attribute(reader.getString(1)));
                }
            } catch (Exception e) {
                LOG.error("Error fetching attributes for table '{}': ", relationName, e);
            }
        }
        return attributes;
    }

    @Override
    public Map<Attribute, List<String>> readRows(int num) {
        connectToDatabase();

        Map<Attribute, List<String>> data = new LinkedHashMap<>();
        List<Attribute> attrs = getAttributes();
        attrs.forEach(a -> data.put(a, new ArrayList<>()));

        String query = String.format("SELECT * FROM %s LIMIT %d OFFSET %d", this.relationName, num, lineCounter);

        try (QueryResponse response = client.query(query).get()) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            while (reader.hasNext()) {
                reader.next();
                List<String> values = new ArrayList<>();
                for (int i = 1; i <= attrs.size(); i++) {
                    values.add(reader.getString(i));
                }
                Record rec = new Record();
                rec.setTuples(values.toArray(new String[0]));
                lineCounter++;
                for (int i = 0; i < attrs.size(); i++) {
                    data.get(attrs.get(i)).add(values.get(i));
                }
                successRecords.inc();
            }

        } catch (Exception e) {
            LOG.error("Error reading rows from table '{}': ", relationName, e);
        }

        return data;
    }

    private void connectToDatabase() {
        if (client == null) {
            try {
                Client.Builder clientBuilder = new Client.Builder()
                .addEndpoint(this.config.getPath())
                .setUsername(this.config.getDbUsername())
                .setPassword(this.config.getDbPassword())
                .compressServerResponse(true)
                .setDefaultDatabase(this.config.getDatabaseName());

                client = clientBuilder.build();
            } catch (Exception e) {
                LOG.error("Error establishing connection to ClickHouse: ", e);
            }
        }
    }

    @Override
    public void close() {
        if (client != null) {
            try {
                client.close();;
            } catch (Exception e) {
                LOG.error("Error closing connection to ClickHouse: ", e);
            }
        }
    }
}
