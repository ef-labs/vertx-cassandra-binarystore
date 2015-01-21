package com.englishtown.vertx.cassandra.binarystore;

import com.englishtown.promises.Promise;
import com.englishtown.vertx.cassandra.CassandraSession;
import io.vertx.core.Vertx;

import javax.inject.Inject;

/**
 * Initializes the binary store and closes it when finished
 */
public class BinaryStoreStarter implements AutoCloseable {

    private final CassandraSession session;
    private final BinaryStoreStatements statements;
    private final Vertx vertx;

    @Inject
    public BinaryStoreStarter(CassandraSession session, BinaryStoreStatements statements, Vertx vertx) {
        this.session = session;
        this.statements = statements;
        this.vertx = vertx;
    }

    public Promise<Void> run() {

        // Get keyspace, default to binarystore
        String keyspace = vertx.getOrCreateContext().config().getString("keyspace", "binarystore");

        return statements.init(keyspace);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws Exception {
        if (session != null) {
            session.close();
        }
    }

}
