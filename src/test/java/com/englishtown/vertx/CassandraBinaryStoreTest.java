package com.englishtown.vertx;

import com.datastax.driver.core.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Container;

import javax.inject.Provider;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link CassandraBinaryStore}
 */
@RunWith(MockitoJUnitRunner.class)
public class CassandraBinaryStoreTest {

    CassandraBinaryStore binaryStore;
    JsonObject config = new JsonObject();
    JsonObject jsonBody = new JsonObject();

    @Mock
    Message<JsonObject> jsonMessage;
    @Mock
    Session session;
    @Mock
    Cluster cluster;
    @Mock
    Metadata metadata;
    @Mock
    Cluster.Builder builder;
    @Mock
    Provider<Cluster.Builder> provider;
    @Mock
    Vertx vertx;
    @Mock
    Container container;
    @Mock
    EventBus eventBus;
    @Mock
    Logger logger;
    @Mock
    PreparedStatement preparedStatement;
    @Mock
    BoundStatement boundStatement;
    @Mock
    ResultSetFuture resultSetFuture;

    @Before
    public void setUp() throws Exception {

        when(jsonMessage.body()).thenReturn(jsonBody);

        when(provider.get()).thenReturn(builder);

        when(builder.addContactPoint(anyString())).thenReturn(builder);
        when(builder.build()).thenReturn(cluster);

        when(cluster.connect()).thenReturn(session);
        when(cluster.getMetadata()).thenReturn(metadata);

        when(container.config()).thenReturn(config);
        when(container.logger()).thenReturn(logger);
        when(vertx.eventBus()).thenReturn(eventBus);

        when(session.prepare(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.bind(anyVararg())).thenReturn(boundStatement);
        when(session.executeAsync(any(Query.class))).thenReturn(resultSetFuture);

        binaryStore = new CassandraBinaryStore(provider);
        binaryStore.setVertx(vertx);
        binaryStore.setContainer(container);

        binaryStore.start();
    }

    @Test
    public void testStart() throws Exception {
        // Start is called during setUp(), just run verifications
        verify(provider).get();
        verify(builder).addContactPoint("127.0.0.1");
        verify(builder).build();
        verify(cluster).connect();

        verify(eventBus).registerHandler(eq(CassandraBinaryStore.DEFAULT_ADDRESS), eq(binaryStore));
        verify(eventBus).registerHandler(eq(CassandraBinaryStore.DEFAULT_ADDRESS + "/saveChunk"), any(Handler.class));

    }

    @Test
    public void testStop() throws Exception {
        binaryStore.stop();
        verify(cluster).shutdown();
    }

    @Test
    public void testEnsureSchema() throws Exception {
        // ensureSchema() is called  from start() during setUp(), just run verifications
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(session, times(3)).execute(captor.capture());
        List<String> queries = captor.getAllValues();

        assertEquals("CREATE KEYSPACE binarystore WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};", queries.get(0));
        assertEquals("CREATE TABLE binarystore.files (id uuid PRIMARY KEY,filename text,contentType text,chunkSize int,length bigint,uploadDate bigint,metadata text);", queries.get(1));
        assertEquals("CREATE TABLE binarystore.chunks (files_id uuid,n int,data blob,PRIMARY KEY (files_id, n));", queries.get(2));
    }

    @Test
    public void testInitPreparedStatements() throws Exception {
        // initPreparedStatements() is called  from start() during setUp(), just run verifications
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(session, times(4)).prepare(captor.capture());
        List<String> queries = captor.getAllValues();

        assertEquals("INSERT INTO binarystore.chunks(files_id,n,data) VALUES (?,?,?);", queries.get(0));
        assertEquals("INSERT INTO binarystore.files(id,length,chunkSize,uploadDate,filename,contentType,metadata) VALUES (?,?,?,?,?,?,?);", queries.get(1));
        assertEquals("SELECT * FROM binarystore.files WHERE id=?;", queries.get(2));
        assertEquals("SELECT data FROM binarystore.chunks WHERE files_id=? AND n=?;", queries.get(3));

    }

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

        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(resultSetFuture).addListener(captor.capture(), any(Executor.class));
        captor.getValue().run();

        ArgumentCaptor<JsonObject> jsonCaptor = ArgumentCaptor.forClass(JsonObject.class);
        verify(jsonMessage).reply(jsonCaptor.capture());

        JsonObject reply = jsonCaptor.getValue();
        assertEquals("ok", reply.getString("status"));
    }

    @Test
    public void testSaveChunk() throws Exception {

    }

    @Test
    public void testGetFile() throws Exception {

    }

    @Test
    public void testGetChunk() throws Exception {

    }

    @Test
    public void testExecuteQuery() throws Exception {

    }

    @Test
    public void testSendError() throws Exception {

    }

    @Test
    public void testSendOK() throws Exception {

    }

    private void verifyError(String message) {

        ArgumentCaptor<JsonObject> captor = ArgumentCaptor.forClass(JsonObject.class);
        verify(jsonMessage).reply(captor.capture());
        JsonObject reply = captor.getValue();

        assertEquals("error", reply.getString("status"));
        assertEquals(message, reply.getString("message"));

    }
}
