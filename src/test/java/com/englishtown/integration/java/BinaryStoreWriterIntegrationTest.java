package com.englishtown.integration.java;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Cluster;
import com.englishtown.vertx.cassandra.CassandraConfigurator;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.englishtown.vertx.cassandra.binarystore.*;
import com.englishtown.vertx.cassandra.binarystore.impl.DefaultBinaryStoreManager;
import com.englishtown.vertx.cassandra.binarystore.impl.DefaultBinaryStoreStatements;
import com.englishtown.vertx.cassandra.binarystore.impl.DefaultBinaryStoreWriter;
import com.englishtown.vertx.cassandra.binarystore.impl.DefaultFileInfo;
import com.englishtown.vertx.cassandra.impl.DefaultCassandraSession;
import com.englishtown.vertx.cassandra.impl.EnvironmentCassandraConfigurator;
import com.englishtown.vertx.cassandra.promises.WhenCassandraSession;
import com.englishtown.vertx.cassandra.promises.impl.DefaultWhenCassandraSession;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.file.AsyncFile;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import javax.inject.Provider;
import java.net.URL;

import static org.vertx.testtools.VertxAssert.*;

/**
 * Integration test for {@link com.englishtown.vertx.cassandra.binarystore.BinaryStoreWriter}
 */
public class BinaryStoreWriterIntegrationTest extends TestVerticle {

    private BinaryStoreWriter binaryStoreWriter;

    @Test
    public void testWrite() throws Exception {

        final String name = "EF_Labs_ENG_logo.JPG";
        final int length = 161966;
        final int chunkSize = 102400;

        URL url = getClass().getResource("/" + name);

        if (url == null) {
            fail("Image file is missing.");
            return;
        }

        vertx.fileSystem().open(url.getPath(), new Handler<AsyncResult<AsyncFile>>() {
            @Override
            public void handle(AsyncResult<AsyncFile> result) {
                if (result.succeeded()) {

                    AsyncFile file = result.result();
                    FileInfo fi = new DefaultFileInfo().setFileName(name).setChunkSize(chunkSize);

                    binaryStoreWriter.write(fi, file, new FutureCallback<FileInfo>() {
                                @Override
                                public void onSuccess(FileInfo result) {
                                    assertNotNull(result);
                                    assertNotNull(result.getId());
                                    assertEquals(name, result.getFileName());
                                    assertEquals(length, result.getLength());
                                    assertEquals(chunkSize, result.getChunkSize());
                                    assertEquals(2, result.getChunkCount());
                                    assertEquals("image/jpeg", result.getContentType());

                                    testComplete();
                                }

                                @Override
                                public void onFailure(Throwable t) {
                                    fail(t.getMessage());
                                }
                            }
                    );

                } else {
                    fail(result.cause().getMessage());
                }
            }
        });

    }

    @Override
    public void start(final Future<Void> startedResult) {

        final Cluster.Builder builder = new Cluster.Builder();
        Provider<Cluster.Builder> builderProvider = new Provider<Cluster.Builder>() {
            @Override
            public Cluster.Builder get() {
                return builder;
            }
        };

        CassandraConfigurator configurator = new EnvironmentCassandraConfigurator(new JsonObject(), container);
        CassandraSession session = new DefaultCassandraSession(builderProvider, configurator, vertx);
        WhenCassandraSession whenSession = new DefaultWhenCassandraSession(session);
        BinaryStoreStatements statements = new DefaultBinaryStoreStatements(whenSession);
        BinaryStoreStarter starter = new BinaryStoreStarter(session, statements, container);
        BinaryStoreManager binaryStoreManager = new DefaultBinaryStoreManager(session, statements, new MetricRegistry());

        binaryStoreWriter = new DefaultBinaryStoreWriter(binaryStoreManager);

        starter.run(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
                if (result.succeeded()) {
                    startedResult.setResult(result.result());
                    start();
                } else {
                    startedResult.setFailure(result.cause());
                    fail();
                }
            }
        });

    }
}
