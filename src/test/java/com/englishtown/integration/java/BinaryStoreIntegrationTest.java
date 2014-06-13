package com.englishtown.integration.java;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Cluster;
import com.englishtown.vertx.cassandra.CassandraConfigurator;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.englishtown.vertx.cassandra.binarystore.*;
import com.englishtown.vertx.cassandra.binarystore.impl.*;
import com.englishtown.vertx.cassandra.impl.DefaultCassandraSession;
import com.englishtown.vertx.cassandra.impl.EnvironmentCassandraConfigurator;
import com.englishtown.vertx.cassandra.promises.WhenCassandraSession;
import com.englishtown.vertx.cassandra.promises.impl.DefaultWhenCassandraSession;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.file.AsyncFile;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import javax.inject.Provider;
import java.net.URL;
import java.util.UUID;

import static org.vertx.testtools.VertxAssert.*;

/**
 * Integration test for {@link com.englishtown.vertx.cassandra.binarystore.BinaryStoreWriter} and {@link com.englishtown.vertx.cassandra.binarystore.BinaryStoreReader}
 */
public class BinaryStoreIntegrationTest extends TestVerticle {

    private BinaryStoreWriter binaryStoreWriter;
    private BinaryStoreReader binaryStoreReader;

    private final String FILE_NAME = "EF_Labs_ENG_logo.JPG";
    private final int FILE_LENGTH = 161966;

    @Test
    public void testWrite() throws Exception {

        final int chunkSize = 102400;

        URL url = getClass().getResource("/" + FILE_NAME);

        if (url == null) {
            fail("Image file is missing.");
            return;
        }

        vertx.fileSystem().open(url.getPath(), new Handler<AsyncResult<AsyncFile>>() {
            @Override
            public void handle(AsyncResult<AsyncFile> result) {
                if (result.succeeded()) {

                    AsyncFile file = result.result();
                    FileInfo fi = new DefaultFileInfo().setFileName(FILE_NAME).setChunkSize(chunkSize);

                    binaryStoreWriter.write(fi, file, new FutureCallback<FileInfo>() {
                                @Override
                                public void onSuccess(FileInfo result) {
                                    assertNotNull(result);
                                    assertNotNull(result.getId());
                                    assertEquals(FILE_NAME, result.getFileName());
                                    assertEquals(FILE_LENGTH, result.getLength());
                                    assertEquals(chunkSize, result.getChunkSize());
                                    assertEquals(2, result.getChunkCount());
                                    assertEquals("image/jpeg", result.getContentType());

                                    testRead(result.getId());
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

    private void testRead(final UUID id) {

        final FileReader reader = binaryStoreReader.read(id);

        final FileReadInfo[] finalFileReadInfo = new FileReadInfo[1];
        final Buffer finalBuffer = new Buffer();
        final FileReader.Result[] finalResult = new FileReader.Result[1];

        reader
                .fileHandler(new Handler<FileReadInfo>() {
                    @Override
                    public void handle(FileReadInfo fileReadInfo) {
                        finalFileReadInfo[0] = fileReadInfo;
                    }
                })
                .dataHandler(new Handler<Buffer>() {
                    @Override
                    public void handle(Buffer buffer) {
                        finalBuffer.appendBuffer(buffer);
                    }
                })
                .exceptionHandler(new Handler<Throwable>() {
                    @Override
                    public void handle(Throwable t) {
                        handleThrowable(t);
                        fail();
                    }
                })
                .resultHandler(new Handler<FileReader.Result>() {
                    @Override
                    public void handle(FileReader.Result result) {
                        finalResult[0] = result;
                    }
                })
                .endHandler(new Handler<Void>() {
                    @Override
                    public void handle(Void event) {

                        FileReader.Result result = finalResult[0];
                        assertEquals(FileReader.Result.OK, result);

                        FileReadInfo fileReadInfo = finalFileReadInfo[0];
                        assertNotNull(fileReadInfo);
                        FileInfo fileInfo = fileReadInfo.getFile();

                        assertEquals(id, fileInfo.getId());
                        assertEquals(FILE_NAME, fileInfo.getFileName());
                        assertEquals("image/jpeg", fileInfo.getContentType());
                        assertEquals(FILE_LENGTH, fileInfo.getLength());

                        assertEquals(FILE_LENGTH, finalBuffer.length());

                        testComplete();

                    }
                });

        reader.pause();

        vertx.setTimer(1000, new Handler<Long>() {
            @Override
            public void handle(Long event) {
                reader.resume();
            }
        });

    }

    @Override
    public void start(final Future<Void> startedResult) {

        final Cluster.Builder builder = new Cluster.Builder();

        JsonObject config = IntegrationTestHelper.loadConfig();
        container.config().mergeIn(config);

        CassandraConfigurator configurator = new EnvironmentCassandraConfigurator(container);
        CassandraSession session = new DefaultCassandraSession(builder, configurator, vertx);
        WhenCassandraSession whenSession = new DefaultWhenCassandraSession(session);
        BinaryStoreStatements statements = new DefaultBinaryStoreStatements(whenSession);
        BinaryStoreStarter starter = new BinaryStoreStarter(session, statements, container);
        BinaryStoreManager binaryStoreManager = new DefaultBinaryStoreManager(session, statements, new MetricRegistry());

        binaryStoreWriter = new DefaultBinaryStoreWriter(binaryStoreManager);
        binaryStoreReader = new DefaultBinaryStoreReader(binaryStoreManager, container);

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
