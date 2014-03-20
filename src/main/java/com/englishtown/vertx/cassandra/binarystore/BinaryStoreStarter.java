package com.englishtown.vertx.cassandra.binarystore;

import com.englishtown.vertx.cassandra.CassandraSession;
import com.google.common.util.concurrent.FutureCallback;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.DefaultFutureResult;
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

    public void run(final Handler<AsyncResult<Void>> done) {

        // Get keyspace, default to binarystore
        String keyspace = container.config().getString("keyspace", "binarystore");

        statements.init(keyspace, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void result) {
                done.handle(new DefaultFutureResult<>((Void) null));
            }

            @Override
            public void onFailure(Throwable t) {
                done.handle(new DefaultFutureResult<Void>(t));
            }
        });

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
