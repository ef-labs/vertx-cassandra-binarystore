package com.englishtown.vertx.cassandra.binarystore;

import com.englishtown.promises.Promise;
import com.englishtown.vertx.cassandra.CassandraSession;
import org.vertx.java.platform.Container;

import javax.inject.Inject;

/**
 * Initializes the binary store and closes it when finished
 */
public class BinaryStoreStarter implements AutoCloseable {

    private final CassandraSession session;
    private final BinaryStoreStatements statements;
    private final Container container;

    @Inject
    public BinaryStoreStarter(CassandraSession session, BinaryStoreStatements statements, Container container) {
        this.session = session;
        this.statements = statements;
        this.container = container;
    }

    public Promise<Void> run() {

        // Get keyspace, default to binarystore
        String keyspace = container.config().getString("keyspace", "binarystore");

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
