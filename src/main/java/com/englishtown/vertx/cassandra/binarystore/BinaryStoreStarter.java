package com.englishtown.vertx.cassandra.binarystore;

import com.englishtown.promises.Promise;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.google.common.base.Strings;
import io.vertx.core.Vertx;

import javax.inject.Inject;

/**
 * Initializes the binary store and closes it when finished
 */
public class BinaryStoreStarter implements AutoCloseable {

    public static final String ENV_VAR_KEYSPACE = "BINARYSTORE_KEYSPACE";
    public static final String DEFAULT_KEYSPACE = "binarystore";

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
        String keyspace = System.getenv(ENV_VAR_KEYSPACE);

        // Get keyspace, default to binarystore
        if (Strings.isNullOrEmpty(keyspace)) {
            keyspace = vertx.getOrCreateContext().config().getString("keyspace", DEFAULT_KEYSPACE);
        }

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
