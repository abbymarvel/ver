package ddprofiler.sources.implementations;

import static com.codahale.metrics.MetricRegistry.name;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseValue;
import com.codahale.metrics.Counter;

import co.elastic.clients.elasticsearch.sql.QueryResponse;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

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
    private ClickHouseNode server;
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

        if (server == null) {
            connectToDatabase();
        }

        List<Source> tasks = new ArrayList<>();
        ClickHouseSourceConfig clickHouseConfig = (ClickHouseSourceConfig) config;
        String connectionString = clickHouseConfig.getPath();

        LOG.info("Connecting to ClickHouse at: {}", connectionString);

        String query = String.format("SHOW TABLES");

        try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol());
                ClickHouseResponse response = client.read(server)
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .query(query).execute().get()) {

            for (ClickHouseRecord record : response.records()) {
                String tableName = record.getValue(0).asString();
                System.out.println("Table name:" + tableName);

                ClickHouseSource task = new ClickHouseSource(clickHouseConfig.getDatabaseName(),
                        record.getValue(0).asString(),
                        config);
                tasks.add(task);
            }
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
        if (server == null) {
            connectToDatabase();
        }

        if (attributes == null) {
            attributes = new ArrayList<>();

            String query = String.format("SELECT * FROM %s LIMIT 1;", this.relationName);

            try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol());
                    ClickHouseResponse response = client.read(server)
                            .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                            .query(query).execute().get()) {
                for (ClickHouseColumn column : response.getColumns()) {
                    String columnName = column.getColumnName();
                    attributes.add(new Attribute(columnName));
                    LOG.debug("Attributes: " + columnName);
                }

            } catch (Exception e) {
                LOG.error("Error fetching attributes for table '{}': ", relationName, e);
            }
        }
        return attributes;
    }

    @Override
    public Map<Attribute, List<String>> readRows(int num) {
        if (server == null) {
            connectToDatabase();
        }

        Map<Attribute, List<String>> data = new LinkedHashMap<>();
        List<Attribute> attrs = getAttributes();

        // Initialize data structure to store rows
        attrs.forEach(a -> data.put(a, new ArrayList<>()));

        List<Record> recs = new ArrayList<>();

        // Read rows from ClickHouse
        boolean read_lines = this.read(num, recs);

        // If no rows were read, return null
        if (!read_lines) {
            return null;
        }

        // Populate the data structure
        for (Record record : recs) {
            List<String> values = record.getTuples();
            for (int i = 0; i < values.size(); i++) {
                data.get(attrs.get(i)).add(values.get(i));
            }
        }

        return data;
    }

    private boolean read(int numRecords, List<Record> rec_list) {
        boolean read_lines = false;

        try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol());
                ClickHouseResponse response = client.read(server)
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .query(String.format("SELECT * FROM %s LIMIT %d OFFSET %d",
                                this.relationName, numRecords, lineCounter)) // Use OFFSET to skip already-read rows
                        .execute().get()) {

            for (ClickHouseRecord record : response.records()) {
                read_lines = true;
                Record rec = new Record();

                List<String> values = new ArrayList<>();
                for (int i = 0; i < record.size(); i++) {
                    values.add(record.getValue(i).asString());
                }
                rec.setTuples(values);
                rec_list.add(rec);

                lineCounter++; // Increment line counter to track the offset
            }

        } catch (Exception e) {
            LOG.error("Error reading from ClickHouse: ", e);
        }

        return read_lines;
    }

    private void connectToDatabase() {
        if (server == null) {
            try {
                server = ClickHouseNode.builder()
                        .host(config.getDbServerIp())
                        .port(ClickHouseProtocol.HTTP, config.getDbServerPort())
                        .database(config.getDatabaseName()).credentials(ClickHouseCredentials.fromUserAndPassword(
                                config.getDbUsername(), config.getDbPassword()))
                        .build();
            } catch (Exception e) {
                LOG.error("Error establishing connection to ClickHouse: ", e);
            }
        }
    }

    @Override
    public void close() {
        if (server != null) {
            try {
                server = null;
            } catch (Exception e) {
                LOG.error("Error closing connection to ClickHouse: ", e);
            }
        }
    }
}
