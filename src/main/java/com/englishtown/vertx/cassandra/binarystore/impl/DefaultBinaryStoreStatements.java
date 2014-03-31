package com.englishtown.vertx.cassandra.binarystore.impl;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.englishtown.promises.*;
import com.englishtown.vertx.cassandra.binarystore.BinaryStoreStatements;
import com.englishtown.vertx.cassandra.promises.WhenCassandraSession;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.FutureCallback;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

/**
 * Default implementation of {@link com.englishtown.vertx.cassandra.binarystore.BinaryStoreStatements}
 */
public class DefaultBinaryStoreStatements implements BinaryStoreStatements {

    private final WhenCassandraSession session;
    private boolean isInitialized;
    private String keyspace;
    private PreparedStatement storeChunk;
    private PreparedStatement storeFile;
    private PreparedStatement loadChunk;
    private PreparedStatement loadFile;


    @Inject
    public DefaultBinaryStoreStatements(WhenCassandraSession session) {
        this.session = session;
    }

    @Override
    public boolean isInitialized() {
        return isInitialized;
    }

    @Override
    public void init(final String keyspace, final FutureCallback<Void> callback) {

        if (Strings.isNullOrEmpty(keyspace)) {
            callback.onFailure(new RuntimeException("keyspace was missing"));
            return;
        }

        this.keyspace = keyspace;

        ensureKeyspace().then(
                new FulfilledRunnable<KeyspaceMetadata>() {
                    @Override
                    public Promise<KeyspaceMetadata> run(KeyspaceMetadata kmd) {

                        When<ResultSet> when = new When<>();
                        List<Promise<ResultSet>> promises = new ArrayList<>();

                        ensureTables(promises, kmd);

                        if (promises.size() == 0) {
                            initPreparedStatements(callback);
                            return null;
                        }

                        when.all(promises).then(
                                new FulfilledRunnable<List<? extends ResultSet>>() {
                                    @Override
                                    public Promise<List<? extends ResultSet>> run(List<? extends ResultSet> resultSets) {
                                        initPreparedStatements(callback);
                                        return null;
                                    }
                                },
                                new RejectedRunnable<List<? extends ResultSet>>() {
                                    @Override
                                    public Promise<List<? extends ResultSet>> run(Value<List<? extends ResultSet>> listValue) {
                                        callback.onFailure(listValue.getCause());
                                        return null;
                                    }
                                }
                        );

                        return null;
                    }
                },
                new RejectedRunnable<KeyspaceMetadata>() {
                    @Override
                    public Promise<KeyspaceMetadata> run(Value<KeyspaceMetadata> value) {
                        callback.onFailure(value.getCause());
                        return null;
                    }
                }
        );

    }

    @Override
    public String getKeyspace() {
        return keyspace;
    }

    @Override
    public PreparedStatement getLoadChunk() {
        return loadChunk;
    }

    @Override
    public BinaryStoreStatements setLoadChunk(PreparedStatement loadChunk) {
        this.loadChunk = loadChunk;
        return this;
    }

    @Override
    public PreparedStatement getLoadFile() {
        return loadFile;
    }

    @Override
    public BinaryStoreStatements setLoadFile(PreparedStatement loadFile) {
        this.loadFile = loadFile;
        return this;
    }

    @Override
    public PreparedStatement getStoreChunk() {
        return storeChunk;
    }

    @Override
    public BinaryStoreStatements setStoreChunk(PreparedStatement storeChunk) {
        this.storeChunk = storeChunk;
        return this;
    }

    @Override
    public PreparedStatement getStoreFile() {
        return storeFile;
    }

    @Override
    public BinaryStoreStatements setStoreFile(PreparedStatement storeFile) {
        this.storeFile = storeFile;
        return this;
    }

    Promise<KeyspaceMetadata> ensureKeyspace() {

        When<KeyspaceMetadata> when = new When<>();

        final Metadata metadata = session.getMetadata();

        // Ensure the keyspace exists
        KeyspaceMetadata kmd = metadata.getKeyspace(keyspace);

        if (kmd != null) {
            return when.resolve(kmd);
        }

        LoadBalancingPolicy lbPolicy = session.getCluster().getConfiguration().getPolicies().getLoadBalancingPolicy();
        String dc = null;
        int count = 0;

        for (Host host : metadata.getAllHosts()) {
            if (lbPolicy.distance(host) == HostDistance.LOCAL) {
                dc = host.getDatacenter();
                count++;
            }
        }

        StringBuilder replication = new StringBuilder();

        if (count > 0) {
            replication.append("= {'class':'NetworkTopologyStrategy', '")
                    .append(dc)
                    .append("':")
                    .append((count > 2 ? 3 : count == 2 ? 2 : 1))
                    .append("};");
        } else {
            replication.append("= {'class':'SimpleStrategy', 'replication_factor':3};");
        }

        final Deferred<KeyspaceMetadata> d = when.defer();
        String cql = "CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH replication " + replication.toString();

        session.executeAsync(new SimpleStatement(cql)).then(
                new FulfilledRunnable<ResultSet>() {
                    @Override
                    public Promise<ResultSet> run(ResultSet value) {
                        d.getResolver().resolve(metadata.getKeyspace(keyspace));
                        return null;
                    }
                },
                new RejectedRunnable<ResultSet>() {
                    @Override
                    public Promise<ResultSet> run(Value<ResultSet> value) {
                        d.getResolver().reject(value.getCause());
                        return null;
                    }
                }
        );

        return d.getPromise();
    }

    void ensureTables(List<Promise<ResultSet>> promises, KeyspaceMetadata kmd) {

        if (kmd == null || kmd.getTable("files") == null) {
            String cql =
                    "CREATE TABLE IF NOT EXISTS " + keyspace + ".files (" +
                            "id uuid PRIMARY KEY," +
                            "filename text," +
                            "contentType text," +
                            "chunkSize int," +
                            "length bigint," +
                            "uploadDate bigint," +
                            "metadata map<text, text>" +
                            ");";

            promises.add(session.executeAsync(new SimpleStatement(cql)));
        }

        if (kmd == null || kmd.getTable("chunks") == null) {
            String cql =
                    "CREATE TABLE IF NOT EXISTS " + keyspace + ".chunks (" +
                            "files_id uuid," +
                            "n int," +
                            "data blob," +
                            "PRIMARY KEY (files_id, n)" +
                            ");";

            promises.add(session.executeAsync(new SimpleStatement(cql)));
        }

    }

    public void initPreparedStatements(final FutureCallback<Void> callback) {

        List<Promise<PreparedStatement>> promises = new ArrayList<>();

        RegularStatement query = QueryBuilder
                .insertInto(keyspace, "files")
                .value("id", bindMarker())
                .value("length", bindMarker())
                .value("chunkSize", bindMarker())
                .value("uploadDate", bindMarker())
                .value("filename", bindMarker())
                .value("contentType", bindMarker())
                .value("metadata", bindMarker());

        promises.add(session.prepareAsync(query));

        query = QueryBuilder
                .select()
                .all()
                .from(keyspace, "files")
                .where(eq("id", bindMarker()));

        promises.add(session.prepareAsync(query));

        query = QueryBuilder
                .insertInto(keyspace, "chunks")
                .value("files_id", bindMarker())
                .value("n", bindMarker())
                .value("data", bindMarker());

        promises.add(session.prepareAsync(query));

        query = QueryBuilder
                .select("data")
                .from(keyspace, "chunks")
                .where(eq("files_id", bindMarker()))
                .and(eq("n", bindMarker()));

        promises.add(session.prepareAsync(query));

        When<PreparedStatement> when = new When<>();
        when.all(promises).then(
                new FulfilledRunnable<List<? extends PreparedStatement>>() {
                    @Override
                    public Promise<List<? extends PreparedStatement>> run(List<? extends PreparedStatement> preparedStatements) {
                        setStoreFile(preparedStatements.get(0));
                        setLoadFile(preparedStatements.get(1));
                        setStoreChunk(preparedStatements.get(2));
                        setLoadChunk(preparedStatements.get(3));

                        isInitialized = true;
                        callback.onSuccess(null);
                        return null;
                    }
                },
                new RejectedRunnable<List<? extends PreparedStatement>>() {
                    @Override
                    public Promise<List<? extends PreparedStatement>> run(Value<List<? extends PreparedStatement>> listValue) {
                        callback.onFailure(listValue.getCause());
                        return null;
                    }
                }
        );
    }

}
