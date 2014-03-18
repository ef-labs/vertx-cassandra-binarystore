package com.englishtown.vertx.cassandra.binarystore;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.englishtown.vertx.cassandra.CassandraSession;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.platform.Container;

import javax.inject.Inject;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

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

    public void run(Handler<AsyncResult<Void>> done) {

        // Get keyspace, default to binarystore
        String keyspace = container.config().getString("keyspace", "binarystore");
        statements.setKeyspace(keyspace);

        ensureSchema(keyspace);
        initPreparedStatements(keyspace);

        // TODO: Make start async
        done.handle(new DefaultFutureResult<>((Void) null));
    }

    public void ensureSchema(String keyspace) {

        Metadata metadata = session.getMetadata();

        // Ensure the keyspace exists
        KeyspaceMetadata kmd = metadata.getKeyspace(keyspace);
        if (kmd == null) {
            try {
                session.execute("CREATE KEYSPACE " + keyspace + " WITH replication " +
                        "= {'class':'SimpleStrategy', 'replication_factor':3};");
            } catch (AlreadyExistsException e) {
                // OK if it already exists
            }
        }

        if (kmd == null || kmd.getTable("files") == null) {
            try {
                session.execute(
                        "CREATE TABLE " + keyspace + ".files (" +
                                "id uuid PRIMARY KEY," +
                                "filename text," +
                                "contentType text," +
                                "chunkSize int," +
                                "length bigint," +
                                "uploadDate bigint," +
                                "metadata map<text, text>" +
                                ");");

            } catch (AlreadyExistsException e) {
                // OK if it already exists
            }
        }

        if (kmd == null || kmd.getTable("chunks") == null) {
            try {
                session.execute(
                        "CREATE TABLE " + keyspace + ".chunks (" +
                                "files_id uuid," +
                                "n int," +
                                "data blob," +
                                "PRIMARY KEY (files_id, n)" +
                                ");");

            } catch (AlreadyExistsException e) {
                // OK if it already exists
            }
        }

    }

    public void initPreparedStatements(String keyspace) {

        String query = QueryBuilder
                .insertInto(keyspace, "chunks")
                .value("files_id", bindMarker())
                .value("n", bindMarker())
                .value("data", bindMarker())
                .getQueryString();

        statements.setStoreChunk(session.prepare(query));

        query = QueryBuilder
                .insertInto(keyspace, "files")
                .value("id", bindMarker())
                .value("length", bindMarker())
                .value("chunkSize", bindMarker())
                .value("uploadDate", bindMarker())
                .value("filename", bindMarker())
                .value("contentType", bindMarker())
                .value("metadata", bindMarker())
                .getQueryString();

        statements.setStoreFile(session.prepare(query));

        query = QueryBuilder
                .select()
                .all()
                .from(keyspace, "files")
                .where(eq("id", bindMarker()))
                .getQueryString();

        statements.setLoadFile(session.prepare(query));

        query = QueryBuilder
                .select("data")
                .from(keyspace, "chunks")
                .where(eq("files_id", bindMarker()))
                .and(eq("n", bindMarker()))
                .getQueryString();

        statements.setLoadChunk(session.prepare(query));

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
