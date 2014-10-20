package com.englishtown.integration.java;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Cluster;
import com.englishtown.promises.When;
import com.englishtown.promises.WhenFactory;
import com.englishtown.vertx.cassandra.CassandraConfigurator;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.englishtown.vertx.cassandra.binarystore.*;
import com.englishtown.vertx.cassandra.binarystore.impl.*;
import com.englishtown.vertx.cassandra.impl.DefaultCassandraSession;
import com.englishtown.vertx.cassandra.impl.EnvironmentCassandraConfigurator;
import com.englishtown.vertx.cassandra.promises.WhenCassandraSession;
import com.englishtown.vertx.cassandra.promises.impl.DefaultWhenCassandraSession;
import org.junit.Test;
import org.vertx.java.core.Future;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.file.AsyncFile;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

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

        vertx.fileSystem().open(url.getPath(), result1 -> {
            if (result1.succeeded()) {

                AsyncFile file = result1.result();
                FileInfo fi = new DefaultFileInfo().setFileName(FILE_NAME).setChunkSize(chunkSize);

                binaryStoreWriter.write(fi, file)
                        .then(result2 -> {
                            assertNotNull(result2);
                            assertNotNull(result2.getId());
                            assertEquals(FILE_NAME, result2.getFileName());
                            assertEquals(FILE_LENGTH, result2.getLength());
                            assertEquals(chunkSize, result2.getChunkSize());
                            assertEquals(2, result2.getChunkCount());
                            assertEquals("image/jpeg", result2.getContentType());

                            testRead(result2.getId());
                            return null;
                        })
                        .otherwise(t -> {
                            fail(t.getMessage());
                            return null;
                        });

            } else {
                fail(result1.cause().getMessage());
            }
        });

    }

    private void testRead(final UUID id) {

        final FileReader reader = binaryStoreReader.read(id);

        final FileReadInfo[] finalFileReadInfo = new FileReadInfo[1];
        final Buffer finalBuffer = new Buffer();
        final FileReader.Result[] finalResult = new FileReader.Result[1];

        reader
                .fileHandler(fileReadInfo -> finalFileReadInfo[0] = fileReadInfo)
                .dataHandler(buffer -> finalBuffer.appendBuffer(buffer))
                .exceptionHandler(t -> {
                    handleThrowable(t);
                    fail();
                })
                .resultHandler(result -> finalResult[0] = result)
                .endHandler(event -> {

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

                });

        reader.pause();

        vertx.setTimer(1000, event -> reader.resume());

    }

    @Override
    public void start(final Future<Void> startedResult) {

        final Cluster.Builder builder = new Cluster.Builder();

        JsonObject config = IntegrationTestHelper.loadConfig();
        container.config().mergeIn(config);
        When when = WhenFactory.createSync();

        CassandraConfigurator configurator = new EnvironmentCassandraConfigurator(container);
        CassandraSession session = new DefaultCassandraSession(builder, configurator, vertx);
        WhenCassandraSession whenSession = new DefaultWhenCassandraSession(session, when);
        BinaryStoreStatements statements = new DefaultBinaryStoreStatements(whenSession, when);
        BinaryStoreStarter starter = new BinaryStoreStarter(session, statements, container);
        BinaryStoreManager binaryStoreManager = new DefaultBinaryStoreManager(whenSession, statements, new MetricRegistry(), when);

        binaryStoreWriter = new DefaultBinaryStoreWriter(binaryStoreManager, when);
        binaryStoreReader = new DefaultBinaryStoreReader(binaryStoreManager, container);

        starter.run()
                .then(aVoid -> {
                    startedResult.setResult(aVoid);
                    start();
                    return null;
                })
                .otherwise(t -> {
                    startedResult.setFailure(t);
                    fail();
                    return null;
                });

    }
}
