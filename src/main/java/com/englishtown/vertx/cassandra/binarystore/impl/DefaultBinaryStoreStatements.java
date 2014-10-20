package com.englishtown.vertx.cassandra.binarystore.impl;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.englishtown.promises.Promise;
import com.englishtown.promises.When;
import com.englishtown.vertx.cassandra.binarystore.BinaryStoreStatements;
import com.englishtown.vertx.cassandra.keyspacebuilder.CreateKeyspace;
import com.englishtown.vertx.cassandra.keyspacebuilder.KeyspaceBuilder;
import com.englishtown.vertx.cassandra.promises.WhenCassandraSession;
import com.englishtown.vertx.cassandra.tablebuilder.PrimaryKeyType;
import com.englishtown.vertx.cassandra.tablebuilder.TableBuilder;
import com.google.common.base.Strings;

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
    private final When when;
    private boolean isInitialized;
    private String keyspace;
    private PreparedStatement storeChunk;
    private PreparedStatement storeFile;
    private PreparedStatement loadChunk;
    private PreparedStatement loadFile;


    @Inject
    public DefaultBinaryStoreStatements(WhenCassandraSession session, When when) {
        this.session = session;
        this.when = when;
    }

    @Override
    public boolean isInitialized() {
        return isInitialized;
    }

    @Override
    public Promise<Void> init(final String keyspace) {

        if (Strings.isNullOrEmpty(keyspace)) {
            return when.reject(new RuntimeException("keyspace was missing"));
        }

        this.keyspace = keyspace;

        return ensureKeyspace()
                .then(kmd -> {
                    List<Promise<ResultSet>> promises = new ArrayList<>();
                    ensureTables(promises, kmd);

                    if (promises.size() == 0) {
                        return initPreparedStatements();
                    }

                    return when.all(promises)
                            .then(resultSets -> {
                                return initPreparedStatements();
                            });

                })
                .then(aVoid -> {
                    isInitialized = true;
                    return null;
                });

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

    private Promise<KeyspaceMetadata> ensureKeyspace() {

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

        CreateKeyspace create = KeyspaceBuilder
                .create(keyspace)
                .ifNotExists();

        if (count > 0) {
            create.networkTopologyStrategy()
                    .dc(dc, (count > 2 ? 3 : count == 2 ? 2 : 1));
        } else {
            create.simpleStrategy(1);
        }

        return session.executeAsync(create)
                .then(value -> {
                    return when.resolve(metadata.getKeyspace(keyspace));
                });

    }

    private void ensureTables(List<Promise<ResultSet>> promises, KeyspaceMetadata kmd) {

        if (kmd == null || kmd.getTable("files") == null) {
            Statement statement = TableBuilder.create(keyspace, "files")
                    .ifNotExists()
                    .column("id", "uuid")
                    .column("filename", "text")
                    .column("contentType", "text")
                    .column("chunkSize", "int")
                    .column("length", "bigint")
                    .column("uploadDate", "bigint")
                    .column("metadata", "map<text, text>")
                    .primaryKey("id");

            promises.add(session.executeAsync(statement));
        }

        if (kmd == null || kmd.getTable("chunks") == null) {
            Statement statement = TableBuilder.create(keyspace, "chunks")
                    .ifNotExists()
                    .column("files_id", "uuid")
                    .column("n", "int")
                    .column("data", "blob")
                    .primaryKey("files_id", PrimaryKeyType.PARTITIONING)
                    .primaryKey("n", PrimaryKeyType.PARTITIONING);

            promises.add(session.executeAsync(statement));
        }

    }

    public Promise<Void> initPreparedStatements() {

        List<Promise<Void>> promises = new ArrayList<>();

        RegularStatement query = QueryBuilder
                .insertInto(keyspace, "files")
                .value("id", bindMarker())
                .value("length", bindMarker())
                .value("chunkSize", bindMarker())
                .value("uploadDate", bindMarker())
                .value("filename", bindMarker())
                .value("contentType", bindMarker())
                .value("metadata", bindMarker());

        promises.add(session.prepareAsync(query)
                .then(ps -> {
                    setStoreFile(ps);
                    return null;
                }));

        query = QueryBuilder
                .select()
                .all()
                .from(keyspace, "files")
                .where(eq("id", bindMarker()));

        promises.add(session.prepareAsync(query).then(ps -> {
            setLoadFile(ps);
            return null;
        }));


        query = QueryBuilder
                .insertInto(keyspace, "chunks")
                .value("files_id", bindMarker())
                .value("n", bindMarker())
                .value("data", bindMarker());

        promises.add(session.prepareAsync(query).then(ps -> {
            setStoreChunk(ps);
            return null;
        }));

        query = QueryBuilder
                .select("data")
                .from(keyspace, "chunks")
                .where(eq("files_id", bindMarker()))
                .and(eq("n", bindMarker()));

        promises.add(session.prepareAsync(query).then(ps -> {
            setLoadChunk(ps);
            return null;
        }));

        return when.all(promises).then(voids -> null);
    }

}
