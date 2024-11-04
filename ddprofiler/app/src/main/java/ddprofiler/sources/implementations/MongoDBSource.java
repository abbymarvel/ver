package ddprofiler.sources.implementations;

import static com.codahale.metrics.MetricRegistry.name;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import metrics.Metrics;
import ddprofiler.sources.Source;
import ddprofiler.sources.SourceType;
import ddprofiler.sources.SourceUtils;
import ddprofiler.sources.config.MongoDBSourceConfig;
import ddprofiler.sources.config.SourceConfig;
import ddprofiler.sources.deprecated.Attribute;
import ddprofiler.sources.deprecated.Record;

public class MongoDBSource implements Source {

    final private Logger LOG = LoggerFactory.getLogger(MongoDBSource.class.getName());

    private int tid;
    private String databaseName;
    private String relationName;
    private MongoDBSourceConfig config;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private List<Attribute> attributes;

    private long lineCounter = 0;
    private Counter errorRecords = Metrics.REG.counter(name(MongoDBSource.class, "error", "records"));
    private Counter successRecords = Metrics.REG.counter(name(MongoDBSource.class, "success", "records"));

    public MongoDBSource() {}

    public MongoDBSource(String databaseName, String relationName, SourceConfig config) {
        this.tid = SourceUtils.computeTaskId(databaseName, relationName);
        this.databaseName = databaseName;
        this.relationName = relationName;
        this.config = (MongoDBSourceConfig) config;
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
        assert (config instanceof MongoDBSourceConfig);
        this.config = (MongoDBSourceConfig) config;

        List<Source> tasks = new ArrayList<>();
        MongoDBSourceConfig mongoDBConfig = (MongoDBSourceConfig) config;
        String uri = mongoDBConfig.getPath();
        LOG.info("Connecting to MongoDB at: {}", uri);

        try (MongoClient client = MongoClients.create(uri)) {
            database = client.getDatabase(mongoDBConfig.getDatabaseName());
            MongoIterable<String> collectionNames = database.listCollectionNames();
            for (String collectionName : collectionNames) {
                MongoDBSource task = new MongoDBSource(mongoDBConfig.getDatabaseName(), collectionName, config);
                tasks.add(task);
            }
            LOG.info("Total collections processed: {}", tasks.size());
        }
        return tasks;
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.mongodb;
    }

    @Override
    public List<Attribute> getAttributes() {
        if (database == null) {
            connectToDatabase();
        }
        
        if (attributes == null) {
            MongoCollection<Document> collection = database.getCollection(this.relationName);
            Document attrDoc = collection.find().first();

            if (attrDoc != null) {
                attributes = new ArrayList<>();
                for (String key : attrDoc.keySet()) {
                    attributes.add(new Attribute(key));
                }
            } else {
                LOG.warn("No documents found in collection '{}'.", relationName);
                attributes = new ArrayList<>();
            }
        }
        return attributes;
    }

    @Override
    public Map<Attribute, List<String>> readRows(int num) {
        if (database == null) {
            connectToDatabase();
        }

        MongoCollection<Document> collection = database.getCollection(this.relationName);
        Map<Attribute, List<String>> data = new LinkedHashMap<>();
        List<Attribute> attrs = getAttributes();

        attrs.forEach(a -> data.put(a, new ArrayList<>()));
        List<Record> recs = new ArrayList<>();
        
        boolean readData = this.read(num, recs);
        if (!readData) {
            return null;
        }

        for (Record r : recs) {
            List<String> values = r.getTuples();
            if (values.size() != attrs.size()) {
                errorRecords.inc();
                continue;
            }
            successRecords.inc();
            for (int i = 0; i < attrs.size(); i++) {
                data.get(attrs.get(i)).add(values.get(i));
            }
        }
        return data;
    }

    private boolean read(int numRecords, List<Record> recList) {
        MongoCollection<Document> collection = database.getCollection(this.relationName);
        boolean hasDocs = false;

        for (int i = 0; i < numRecords; i++) {
            Document doc = collection.find().skip((int) lineCounter).first();
            if (doc == null) break;

            lineCounter++;
            List<String> values = new ArrayList<>();

            doc.values().forEach(value -> values.add(value.toString()));
            Record rec = new Record();
            rec.setTuples(values.toArray(new String[0]));
            recList.add(rec);
            hasDocs = true;
        }
        return hasDocs;
    }

    private void connectToDatabase() {
        String uri = this.config.getPath();
        mongoClient = MongoClients.create(uri);
        database = mongoClient.getDatabase(this.databaseName);
    }

    @Override
    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
