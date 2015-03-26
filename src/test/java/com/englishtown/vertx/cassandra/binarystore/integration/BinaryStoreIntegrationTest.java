package com.englishtown.vertx.cassandra.binarystore.integration;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Cluster;
import com.englishtown.promises.Promise;
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
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.test.core.VertxTestBase;
import org.junit.Test;

import java.net.URL;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;


/**
 * Integration test for {@link com.englishtown.vertx.cassandra.binarystore.BinaryStoreWriter} and {@link com.englishtown.vertx.cassandra.binarystore.BinaryStoreReader}
 */
public class BinaryStoreIntegrationTest extends VertxTestBase {

    private BinaryStoreWriter binaryStoreWriter;
    private BinaryStoreReader binaryStoreReader;

    private final String FILE_NAME = "EF_Labs_ENG_logo.JPG";
    private final int FILE_LENGTH = 161966;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        CountDownLatch latch = new CountDownLatch(1);

        vertx.runOnContext(aVoid -> {

            final Cluster.Builder builder = new Cluster.Builder();

            JsonObject config = IntegrationTestHelper.loadConfig();
            vertx.getOrCreateContext().config().mergeIn(config);
            When when = WhenFactory.createSync();

            CassandraConfigurator configurator = new EnvironmentCassandraConfigurator(vertx, System::getenv);
            CassandraSession session = new DefaultCassandraSession(builder, configurator, vertx);
            WhenCassandraSession whenSession = new DefaultWhenCassandraSession(session, when, vertx);
            BinaryStoreStatements statements = new DefaultBinaryStoreStatements(whenSession, when);
            BinaryStoreStarter starter = new BinaryStoreStarter(session, statements, vertx);
            BinaryStoreManager binaryStoreManager = new DefaultBinaryStoreManager(whenSession, statements, new MetricRegistry(), when);

            binaryStoreWriter = new DefaultBinaryStoreWriter(binaryStoreManager, when);
            binaryStoreReader = new DefaultBinaryStoreReader(binaryStoreManager);

            starter.run()
                    .otherwise(t -> {
                        t.printStackTrace();
                        fail();
                        return null;
                    })
                    .ensure(latch::countDown);

        });

        latch.await();

    }

    @Test
    public void testWrite() throws Exception {

        final int chunkSize = 102400;

        URL url = getClass().getResource("/" + FILE_NAME);

        if (url == null) {
            fail("Image file is missing.");
            return;
        }

        vertx.fileSystem().open(url.getPath(), new OpenOptions(), result -> {
            if (result.succeeded()) {
                testWrite(result.result(), chunkSize);
            } else {
                result.cause().printStackTrace();
                fail(result.cause().getMessage());
            }
        });

        await();

    }

    private Promise<Void> testWrite(AsyncFile file, int chunkSize) {

        FileInfo fi = new FileInfo().setFileName(FILE_NAME).setChunkSize(chunkSize);

        return binaryStoreWriter.write(fi, file)
                .then(result -> {
                    assertNotNull(result);
                    assertNotNull(result.getId());
                    assertEquals(FILE_NAME, result.getFileName());
                    assertEquals(FILE_LENGTH, result.getLength());
                    assertEquals(chunkSize, result.getChunkSize());
                    assertEquals(2, result.getChunkCount());
                    assertEquals("image/jpeg", result.getContentType());

                    testRead(result.getId());
                    return null;
                })
                .otherwise(t -> {
                    t.printStackTrace();
                    fail();
                    return null;
                });

    }

    private void testRead(final UUID id) {

        final FileReader reader = binaryStoreReader.read(id);

        final FileReadInfo[] finalFileReadInfo = new FileReadInfo[1];
        final Buffer finalBuffer = Buffer.buffer();
        final FileReader.Result[] finalResult = new FileReader.Result[1];

        reader
                .fileHandler(fileReadInfo -> finalFileReadInfo[0] = fileReadInfo)
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

                })
                .handler(buffer -> finalBuffer.appendBuffer(buffer))
                .exceptionHandler(t -> {
                    t.printStackTrace();
                    fail();
                });

        reader.pause();

        vertx.setTimer(1000, event -> reader.resume());

    }

}
