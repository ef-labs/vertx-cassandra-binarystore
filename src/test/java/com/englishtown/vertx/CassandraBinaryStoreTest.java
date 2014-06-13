package com.englishtown.vertx;

import com.codahale.metrics.MetricRegistry;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.englishtown.vertx.cassandra.binarystore.BinaryStoreManager;
import com.englishtown.vertx.cassandra.binarystore.BinaryStoreStarter;
import com.englishtown.vertx.cassandra.binarystore.ChunkInfo;
import com.englishtown.vertx.cassandra.binarystore.FileInfo;
import com.englishtown.vertx.cassandra.binarystore.impl.DefaultChunkInfo;
import com.englishtown.vertx.cassandra.binarystore.impl.DefaultFileInfo;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Container;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link CassandraBinaryStore}
 */
@RunWith(MockitoJUnitRunner.class)
public class CassandraBinaryStoreTest {

    CassandraBinaryStore binaryStore;
    JsonObject config = new JsonObject();
    JsonObject jsonBody = new JsonObject();
    MetricRegistry metricRegistry = new MetricRegistry();

    @Mock
    BinaryStoreStarter starter;
    @Mock
    BinaryStoreManager binaryStoreManager;
    @Mock
    Message<JsonObject> jsonMessage;
    @Mock
    Message<Buffer> bufferMessage;
    @Mock
    CassandraSession session;
    @Mock
    Vertx vertx;
    @Mock
    Container container;
    @Mock
    EventBus eventBus;
    @Mock
    Logger logger;
    @Mock
    Future<Void> startedResult;
    @Mock
    AsyncResult<Void> voidAsyncResult;
    @Captor
    ArgumentCaptor<FileInfo> fileInfoCaptor;
    @Captor
    ArgumentCaptor<FutureCallback<Void>> voidCallbackCaptor;
    @Captor
    ArgumentCaptor<FutureCallback<FileInfo>> fileInfoCallbackCaptor;
    @Captor
    ArgumentCaptor<FutureCallback<ChunkInfo>> chunkInfoCallbackCaptor;
    @Captor
    ArgumentCaptor<Handler<AsyncResult<Void>>> asyncResultHandlerCaptor;

    UUID uuid = UUID.fromString("901025d2-af7d-11e3-88fe-425861b86ab6");


    @Before
    public void setUp() throws Exception {

        when(jsonMessage.body()).thenReturn(jsonBody);

        when(container.config()).thenReturn(config);
        when(container.logger()).thenReturn(logger);
        when(vertx.eventBus()).thenReturn(eventBus);


        binaryStore = new CassandraBinaryStore(starter, binaryStoreManager, metricRegistry);
        binaryStore.setVertx(vertx);
        binaryStore.setContainer(container);

        binaryStore.start(startedResult);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testStart() throws Exception {
        // Start is called during setUp(), just run verifications
        verify(starter).run(asyncResultHandlerCaptor.capture());

        when(voidAsyncResult.succeeded()).thenReturn(true);
        asyncResultHandlerCaptor.getValue().handle(voidAsyncResult);
        verify(eventBus).registerHandler(eq(CassandraBinaryStore.DEFAULT_ADDRESS), eq(binaryStore));
        verify(eventBus).registerHandler(eq(CassandraBinaryStore.DEFAULT_ADDRESS + "/saveChunk"), any(Handler.class));

    }

//    @Test
//    public void testStop() throws Exception {
//        binaryStore.stop();
//        verify(session).close();
//    }

//    @Test
//    public void testEnsureSchema() throws Exception {
//        // ensureSchema() is called  from start() during setUp(), just run verifications
//        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
//        verify(session, times(3)).execute(captor.capture());
//        List<String> queries = captor.getAllValues();
//
//        assertEquals("CREATE KEYSPACE binarystore WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};", queries.get(0));
//        assertEquals("CREATE TABLE binarystore.files (id uuid PRIMARY KEY,filename text,contentType text,chunkSize int,length bigint,uploadDate bigint,metadata text);", queries.get(1));
//        assertEquals("CREATE TABLE binarystore.chunks (files_id uuid,n int,data blob,PRIMARY KEY (files_id, n));", queries.get(2));
//    }
//
//    @Test
//    public void testInitPreparedStatements() throws Exception {
//        // initPreparedStatements() is called  from start() during setUp(), just run verifications
//        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
//        verify(session, times(4)).prepare(captor.capture());
//        List<String> queries = captor.getAllValues();
//
//        assertEquals("INSERT INTO binarystore.chunks(files_id,n,data) VALUES (?,?,?);", queries.get(0));
//        assertEquals("INSERT INTO binarystore.files(id,length,chunkSize,uploadDate,filename,contentType,metadata) VALUES (?,?,?,?,?,?,?);", queries.get(1));
//        assertEquals("SELECT * FROM binarystore.files WHERE id=?;", queries.get(2));
//        assertEquals("SELECT data FROM binarystore.chunks WHERE files_id=? AND n=?;", queries.get(3));
//
//    }

    @Test
    public void testHandle_Missing_Action() throws Exception {
        binaryStore.handle(jsonMessage);
        verifyError("action must be specified");
    }

    @Test
    public void testHandle_Invalid_Action() throws Exception {
        jsonBody.putString("action", "invalid");
        binaryStore.handle(jsonMessage);
        verifyError("action invalid is not supported");
    }

    @Test
    public void testSaveFile_Missing_ID() throws Exception {
        binaryStore.saveFile(jsonMessage, jsonBody);
        verifyError("id must be specified");
    }

    @Test
    public void testSaveFile_Invalid_ID() throws Exception {

        jsonBody.putString("id", "not a uuid");

        binaryStore.saveFile(jsonMessage, jsonBody);
        verifyError("id not a uuid is not a valid UUID");
    }

    @Test
    public void testSaveFile_Missing_Length() throws Exception {

        jsonBody.putString("id", UUID.randomUUID().toString());

        binaryStore.saveFile(jsonMessage, jsonBody);
        verifyError("length must be specified");
    }

    @Test
    public void testSaveFile_Invalid_Length() throws Exception {

        jsonBody.putString("id", UUID.randomUUID().toString())
                .putNumber("length", 0);

        binaryStore.saveFile(jsonMessage, jsonBody);
        verifyError("length must be greater than or equal to 1");
    }

    @Test
    public void testSaveFile_Missing_ChunkSize() throws Exception {

        jsonBody.putString("id", UUID.randomUUID().toString())
                .putNumber("length", 1024);

        binaryStore.saveFile(jsonMessage, jsonBody);
        verifyError("chunkSize must be specified");
    }

    @Test
    public void testSaveFile_Invalid_ChunkSize() throws Exception {

        jsonBody.putString("id", UUID.randomUUID().toString())
                .putNumber("length", 1024)
                .putNumber("chunkSize", 0);

        binaryStore.saveFile(jsonMessage, jsonBody);
        verifyError("chunkSize must be greater than or equal to 1");
    }

    @Test
    public void testSaveFile() throws Exception {

        jsonBody.putString("id", UUID.randomUUID().toString())
                .putNumber("length", 1024)
                .putNumber("chunkSize", 1024);

        binaryStore.saveFile(jsonMessage, jsonBody);

        verify(binaryStoreManager).storeFile(fileInfoCaptor.capture(), voidCallbackCaptor.capture());
        voidCallbackCaptor.getValue().onSuccess(null);

        ArgumentCaptor<JsonObject> jsonCaptor = ArgumentCaptor.forClass(JsonObject.class);
        verify(jsonMessage).reply(jsonCaptor.capture());

        JsonObject reply = jsonCaptor.getValue();
        assertEquals("ok", reply.getString("status"));
    }

    @Test
    public void testSaveChunk() throws Exception {


        // Set up buffer and expected chunkinfo
        JsonObject jsonObject = new JsonObject().putString("files_id", uuid.toString()).putNumber("n", 0);
        byte[] jsonBytes = jsonObject.encode().getBytes("UTF-8");

        Buffer buffer = new Buffer().appendInt(jsonBytes.length).appendBytes(jsonBytes).appendBytes("This is some data".getBytes());
        ChunkInfo expectedChunkInfo = new DefaultChunkInfo().setId(uuid).setNum(0).setData("This is some data".getBytes());

        // Set up interactions
        when(bufferMessage.body()).thenReturn(buffer);

        // When we call saveChunk with a buffer
        binaryStore.saveChunk(bufferMessage);

        // Then we expect binaryStoreManager to be called with our expected chunk info
        verify(binaryStoreManager).storeChunk(eq(expectedChunkInfo), voidCallbackCaptor.capture());

        // And then when we call success on the call back
        voidCallbackCaptor.getValue().onSuccess(null);

        // And we expect an ok reply
        ArgumentCaptor<JsonObject> jsonCaptor = ArgumentCaptor.forClass(JsonObject.class);
        verify(bufferMessage).reply(jsonCaptor.capture());

        JsonObject reply = jsonCaptor.getValue();
        assertEquals("ok", reply.getString("status"));
    }

    @Test
    public void testGetFile() throws Exception {

        // Set things up
        JsonObject jsonObject = new JsonObject().putString("id", uuid.toString());
        DefaultFileInfo fileInfo = createFileInfo();
        JsonObject expectedResult = new JsonObject()
                .putString("filename", fileInfo.getFileName())
                .putString("contentType", fileInfo.getContentType())
                .putNumber("length", fileInfo.getLength())
                .putNumber("chunkSize", fileInfo.getChunkSize())
                .putNumber("uploadDate", fileInfo.getUploadDate())
                .putString("status", "ok");

        // When we call getFile on binary store
        binaryStore.getFile(jsonMessage, jsonObject);

        // Then we expect the binaryStoreManager load file method to be called with our ID
        verify(binaryStoreManager).loadFile(eq(uuid), fileInfoCallbackCaptor.capture());

        // When we call the callback's success method with our fileinfo object
        fileInfoCallbackCaptor.getValue().onSuccess(fileInfo);

        // Then we expect an OK reply with our expected JSON object
        ArgumentCaptor<JsonObject> jsonCaptor = ArgumentCaptor.forClass(JsonObject.class);
        verify(jsonMessage).reply(jsonCaptor.capture());

        JsonObject reply = jsonCaptor.getValue();
        assertEquals(expectedResult, reply);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetChunk() throws Exception {
        // Set up expected result and json object
        ChunkInfo chunkInfo = new DefaultChunkInfo().setId(uuid).setNum(0).setData("This is some data".getBytes());
        JsonObject jsonObject = new JsonObject().putString("files_id", uuid.toString()).putNumber("n", 0);

        // When we call getChunk on binary store
        binaryStore.getChunk(jsonMessage, jsonObject);

        // Then we expect the binary store manager's loadchunk method to be called with the right info
        verify(binaryStoreManager).loadChunk(eq(uuid), eq(0), chunkInfoCallbackCaptor.capture());

        // When we call the onSuccess method with our chunkinfo
        chunkInfoCallbackCaptor.getValue().onSuccess(chunkInfo);

        // Then we expect a message reply that contains the correct data
        verify(jsonMessage).reply(eq("This is some data".getBytes()), any(Handler.class));
    }

    private void verifyError(String message) {

        ArgumentCaptor<JsonObject> captor = ArgumentCaptor.forClass(JsonObject.class);
        verify(jsonMessage).reply(captor.capture());
        JsonObject reply = captor.getValue();

        assertEquals("error", reply.getString("status"));
        assertEquals(message, reply.getString("message"));

    }

    private DefaultFileInfo createFileInfo() {
        return new DefaultFileInfo()
                .setChunkSize(100)
                .setContentType("image/jpeg")
                .setFileName("testfile.jpg")
                .setId(uuid)
                .setLength(150L)
                .setUploadDate(123456789L);
    }
}
