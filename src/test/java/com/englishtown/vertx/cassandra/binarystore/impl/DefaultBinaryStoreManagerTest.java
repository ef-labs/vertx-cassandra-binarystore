package com.englishtown.vertx.cassandra.binarystore.impl;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.englishtown.vertx.cassandra.binarystore.BinaryStoreStatements;
import com.englishtown.vertx.cassandra.binarystore.ChunkInfo;
import com.englishtown.vertx.cassandra.binarystore.FileInfo;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultBinaryStoreManagerTest {

    @Mock
    CassandraSession cassandraSession;
    @Mock
    BinaryStoreStatements binaryStoreStatements;
    @Mock
    PreparedStatement preparedStatement;
    @Mock
    BoundStatement boundStatement;
    @Mock
    MetricRegistry registry;
    @Mock
    FutureCallback<Void> storeCallback;
    @Mock
    FutureCallback<FileInfo> loadFileCallback;
    @Mock
    FutureCallback<ChunkInfo> loadChunkCallback;

    // Metrics Mocks
    @Mock
    Timer fileReadTimer;
    @Mock
    Timer.Context fileReadTimerContext;
    @Mock
    Counter fileReadErrorCount;
    @Mock
    Timer fileWriteTimer;
    @Mock
    Timer.Context fileWriteTimerContext;
    @Mock
    Counter fileWriterErrorCount;

    @Mock
    Timer chunkReadTimer;
    @Mock
    Timer.Context chunkReadTimerContext;
    @Mock
    Counter chunkReadErrorCount;
    @Mock
    Timer chunkWriteTimer;
    @Mock
    Timer.Context chunkWriteTimerContext;
    @Mock
    Counter chunkWriterErrorCount;

    @Captor
    ArgumentCaptor<FutureCallback<ResultSet>> execAsyncArgumentCaptor;

    UUID uuid = UUID.fromString("739a6466-adf8-11e3-aca6-425861b86ab6");
    DefaultBinaryStoreManager bsm;

    @Before
    public void setUp() throws Exception {

        when(registry.timer("et.cass.binarystore.files.read.success")).thenReturn(fileReadTimer);
        when(registry.counter("et.cass.binarystore.files.read.errors")).thenReturn(fileReadErrorCount);
        when(registry.timer("et.cass.binarystore.files.write.success")).thenReturn(fileWriteTimer);
        when(registry.counter("et.cass.binarystore.files.write.errors")).thenReturn(fileWriterErrorCount);

        when(registry.timer("et.cass.binarystore.chunks.read.success")).thenReturn(chunkReadTimer);
        when(registry.counter("et.cass.binarystore.chunks.read.errors")).thenReturn(chunkReadErrorCount);
        when(registry.timer("et.cass.binarystore.chunks.write.success")).thenReturn(chunkWriteTimer);
        when(registry.counter("et.cass.binarystore.chunks.write.errors")).thenReturn(chunkWriterErrorCount);

        when(fileReadTimer.time()).thenReturn(fileReadTimerContext);
        when(fileWriteTimer.time()).thenReturn(fileWriteTimerContext);
        when(chunkReadTimer.time()).thenReturn(chunkReadTimerContext);
        when(chunkWriteTimer.time()).thenReturn(chunkWriteTimerContext);

        bsm = new DefaultBinaryStoreManager(cassandraSession, binaryStoreStatements, registry);
    }

    @Test
    public void testMetricInitialisation() throws Exception {
        // When we create a new DefaultBinaryStoreManager instance
        // Then we expect the metrics to be correctly initialised.

        verify(registry).timer(eq("et.cass.binarystore.files.read.success"));
        verify(registry).counter(eq("et.cass.binarystore.files.read.errors"));
        verify(registry).timer(eq("et.cass.binarystore.files.write.success"));
        verify(registry).counter(eq("et.cass.binarystore.files.write.errors"));

        verify(registry).timer(eq("et.cass.binarystore.chunks.read.success"));
        verify(registry).counter(eq("et.cass.binarystore.chunks.read.errors"));
        verify(registry).timer(eq("et.cass.binarystore.chunks.write.success"));
        verify(registry).counter(eq("et.cass.binarystore.chunks.write.errors"));
    }

    @Test
    public void testStoringAFile() throws Exception {

        // Create our FileInfo
        FileInfo fileInfo = createFileInfo();

        // Set up interactions
        when(binaryStoreStatements.getStoreFile()).thenReturn(preparedStatement);
        when(preparedStatement.bind(uuid, 1000L, 100, 123456789L, "testfile.jpg", "image/jpeg", null)).thenReturn(boundStatement);

        // When we call storeFile
        bsm.storeFile(fileInfo, storeCallback);

        // Then we expect the write metric to be started, the binary store to be asked to write the file
        verify(fileWriteTimer).time();

        verify(binaryStoreStatements).getStoreFile();
        verify(preparedStatement).bind(uuid, 1000L, 100, 123456789L, "testfile.jpg", "image/jpeg", null);
        verify(cassandraSession).executeAsync(any(BoundStatement.class), execAsyncArgumentCaptor.capture());

        // When we call the success method on the cassandra callback
        execAsyncArgumentCaptor.getValue().onSuccess(null);

        // Then we expect the timer to be stopped and for our callback to have the success method called
        // We also ensure that the error count was *not* called
        verifyZeroInteractions(fileWriterErrorCount);
        verify(fileWriteTimerContext).stop();
        verify(storeCallback).onSuccess(null);
    }

    @Test
    public void testStoringAFileUnsuccessfully() throws Exception {
        // Create our FileInfo
        FileInfo fileInfo = createFileInfo();

        // Set up interactions
        when(binaryStoreStatements.getStoreFile()).thenReturn(preparedStatement);
        when(preparedStatement.bind(uuid, 1000L, 100, 123456789L, "testfile.jpg", "image/jpeg", null)).thenReturn(boundStatement);

        // When we call storeFile
        bsm.storeFile(fileInfo, storeCallback);

        // Capture the cassandra callback
        verify(cassandraSession).executeAsync(any(BoundStatement.class), execAsyncArgumentCaptor.capture());

        // And then call the onFailure method on the cassandra callback
        Throwable t = new Throwable("This is an error");
        execAsyncArgumentCaptor.getValue().onFailure(t);

        // Then we expect the timer to be stopped and for our callback to have the success method called
        // We also ensure that the error count was incremented.
        verify(fileWriterErrorCount).inc();
        verify(fileWriteTimerContext).stop();
        verify(storeCallback).onFailure(t);
    }

    @Test
    public void testStoringAChunk() throws Exception {
        // Create our chunk info
        ByteBuffer byteBuffer = ByteBuffer.wrap("This is some data".getBytes());
        ChunkInfo chunkInfo = new DefaultChunkInfo().setId(uuid).setNum(1).setData(byteBuffer.array());

        // Set up interactions
        when(binaryStoreStatements.getStoreChunk()).thenReturn(preparedStatement);
        when(preparedStatement.bind(uuid, 1, byteBuffer)).thenReturn(boundStatement);

        // When we call storeChunk
        bsm.storeChunk(chunkInfo, storeCallback);

        // Capture the cassandra callback
        verify(cassandraSession).executeAsync(any(BoundStatement.class), execAsyncArgumentCaptor.capture());

        // And then call the onFailure method on the cassandra callback
        Throwable t = new Throwable("This is an error");
        execAsyncArgumentCaptor.getValue().onFailure(t);

        // Then we expect the timer to be stopped and for our callback to have the success method called
        // We also ensure that the error count was incremented.
        verify(chunkWriterErrorCount).inc();
        verify(chunkWriteTimerContext).stop();
        verify(storeCallback).onFailure(t);
    }

    @Test
    public void testStoringAChunkUnsuccessfully() throws Exception {
        // Create our chunk info
        ByteBuffer byteBuffer = ByteBuffer.wrap("This is some data".getBytes());
        ChunkInfo chunkInfo = new DefaultChunkInfo().setId(uuid).setNum(1).setData(byteBuffer.array());

        // Set up interactions
        when(binaryStoreStatements.getStoreChunk()).thenReturn(preparedStatement);
        when(preparedStatement.bind(uuid, 1, byteBuffer)).thenReturn(boundStatement);

        // When we call storeChunk
        bsm.storeChunk(chunkInfo, storeCallback);

        // Then we expect the write metric to be started, the binary store to be asked to write the file
        verify(chunkWriteTimer).time();

        verify(binaryStoreStatements).getStoreChunk();
        verify(preparedStatement).bind(uuid, 1, byteBuffer);
        verify(cassandraSession).executeAsync(any(BoundStatement.class), execAsyncArgumentCaptor.capture());

        // When we call the success method on the cassandra callback
        execAsyncArgumentCaptor.getValue().onSuccess(null);

        // Then we expect the timer to be stopped and for our callback to have the success method called
        // We also ensure that the error count was *not* called
        verifyZeroInteractions(chunkWriterErrorCount);
        verify(chunkWriteTimerContext).stop();
        verify(storeCallback).onSuccess(null);
    }

    @Test
    public void testLoadingAFile() throws Exception {
        // Set up the FileInfo to compare at the end
        FileInfo fileInfo = createFileInfo();

        // Set up interactions
        when(binaryStoreStatements.getLoadFile()).thenReturn(preparedStatement);
        when(preparedStatement.bind(uuid)).thenReturn(boundStatement);

        ResultSet resultSet = mock(ResultSet.class);
        Row row = mock(Row.class);
        when(resultSet.one()).thenReturn(row);

        when(row.getString("filename")).thenReturn("testfile.jpg");
        when(row.getString("contentType")).thenReturn("image/jpeg");
        when(row.getLong("length")).thenReturn(1000L);
        when(row.getInt("chunkSize")).thenReturn(100);
        when(row.getLong("uploadDate")).thenReturn(123456789L);
        when(row.getMap("metadata", String.class, String.class)).thenReturn(null);

        // When we call loadFile
        bsm.loadFile(uuid, loadFileCallback);

        // Then we expect the read metric to be started and the binary store to be asked to load the file
        verify(fileReadTimer).time();

        verify(binaryStoreStatements).getLoadFile();
        verify(preparedStatement).bind(uuid);
        verify(cassandraSession).executeAsync(any(BoundStatement.class), execAsyncArgumentCaptor.capture());

        // When we call the onSuccess method on the callback with our mocked ResultSet
        execAsyncArgumentCaptor.getValue().onSuccess(resultSet);

        // Then we expect our row to be read, the timer to be stopped and our callback's success method to be called
        // with the correct FileInfo object.
        verifyZeroInteractions(fileReadErrorCount);
        verify(fileReadTimerContext).stop();
        verify(loadFileCallback).onSuccess(eq(fileInfo));
    }

    @Test
    public void testLoadingAFileUnsuccesfully() throws Exception {
        // Set up interactions
        when(binaryStoreStatements.getLoadFile()).thenReturn(preparedStatement);
        when(preparedStatement.bind(uuid)).thenReturn(boundStatement);

        // When we call loadFile
        bsm.loadFile(uuid, loadFileCallback);

        // and when we call the onFailure method on the callback, after capturing it.
        verify(cassandraSession).executeAsync(any(BoundStatement.class), execAsyncArgumentCaptor.capture());

        Throwable t = new Throwable("This was an error.");
        execAsyncArgumentCaptor.getValue().onFailure(t);

        // Then we expect our error count to be incremented, the timer to be stopped and for our failure method on our callback
        // to be called.
        verify(fileReadErrorCount).inc();
        verify(fileReadTimerContext).stop();
        verify(loadFileCallback).onFailure(t);
    }

    @Test
    public void testLoadingAFileThatDoesNotExist() throws Exception {
        // Set up interactions
        when(binaryStoreStatements.getLoadFile()).thenReturn(preparedStatement);
        when(preparedStatement.bind(uuid)).thenReturn(boundStatement);

        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.one()).thenReturn(null);

        // When we call loadFile
        bsm.loadFile(uuid, loadFileCallback);

        // When we call the onSuccess method on the callback with an empty ResultSet
        verify(cassandraSession).executeAsync(any(BoundStatement.class), execAsyncArgumentCaptor.capture());
        execAsyncArgumentCaptor.getValue().onSuccess(resultSet);

        // Then we expect there to be no interactions with the error count, for the timer to be stopped and for our
        // callback to have its success method called with null
        verifyZeroInteractions(fileReadErrorCount);
        verify(fileReadTimerContext).stop();
        verify(loadFileCallback).onSuccess(null);
    }

    @Test
    public void testLoadingAChunk() throws Exception {
        // Set the chunkinfo to compare at the end
        ChunkInfo chunkInfo = new DefaultChunkInfo().setId(uuid).setNum(1).setData("This is some data".getBytes());

        // Set up interactions
        when(binaryStoreStatements.getLoadChunk()).thenReturn(preparedStatement);
        when(preparedStatement.bind(uuid, 1)).thenReturn(boundStatement);

        ResultSet resultSet = mock(ResultSet.class);
        Row row = mock(Row.class);
        when(resultSet.one()).thenReturn(row);

        when(row.getBytes("data")).thenReturn(ByteBuffer.wrap("This is some data".getBytes()));

        // When we call loadChunk
        bsm.loadChunk(uuid, 1, loadChunkCallback);

        // Then we expect the read metricto be started and the binary store to be asked to load the chunk
        verify(chunkReadTimer).time();

        verify(binaryStoreStatements).getLoadChunk();
        verify(preparedStatement).bind(uuid, 1);
        verify(cassandraSession).executeAsync(any(BoundStatement.class), execAsyncArgumentCaptor.capture());

        // When we call the onSuccess method on the callback with our mocked ResultSet
        execAsyncArgumentCaptor.getValue().onSuccess(resultSet);

        // Then we expect our row to be read, the timer to be stopped and our callback's success method to be called
        // with the correct ChunkInfo object.
        verifyZeroInteractions(chunkReadErrorCount);
        verify(chunkReadTimerContext).stop();
        verify(loadChunkCallback).onSuccess(eq(chunkInfo));
    }

    @Test
    public void testLoadingAChunkUnsuccessfully() throws Exception {
        // Set up interactions
        when(binaryStoreStatements.getLoadChunk()).thenReturn(preparedStatement);
        when(preparedStatement.bind(uuid, 1)).thenReturn(boundStatement);

        // When we call loadChunk
        bsm.loadChunk(uuid, 1, loadChunkCallback);

        // and when we call the onFailure method on the callback, after capturing it.
        verify(cassandraSession).executeAsync(any(BoundStatement.class), execAsyncArgumentCaptor.capture());

        Throwable t = new Throwable("This was an error.");
        execAsyncArgumentCaptor.getValue().onFailure(t);

        // Then we expect the chunkErrorCount to be incremented, the timer to be stopped and our callback's onFailure
        // method to be called.
        verify(chunkReadErrorCount).inc();
        verify(chunkReadTimerContext).stop();
        verify(loadChunkCallback).onFailure(t);
    }

    @Test
    public void testLoadingAChunkThatDoesNotExist() throws Exception {
        // Set up interactions
        when(binaryStoreStatements.getLoadChunk()).thenReturn(preparedStatement);
        when(preparedStatement.bind(uuid, 1)).thenReturn(boundStatement);

        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.one()).thenReturn(null);

        // When we call loadChunk
        bsm.loadChunk(uuid, 1, loadChunkCallback);

        // and When we call the onSuccess method on the callback with an empty ResultSet
        verify(cassandraSession).executeAsync(any(BoundStatement.class), execAsyncArgumentCaptor.capture());
        execAsyncArgumentCaptor.getValue().onSuccess(resultSet);


        // Then we expect there to be no interactions with the error count, for the timer to be stopped and for
        // our callback to have its success method called with null
        verifyZeroInteractions(chunkReadErrorCount);
        verify(chunkReadTimerContext).stop();
        verify(loadChunkCallback).onSuccess(null);
    }

    private FileInfo createFileInfo() {
        return new DefaultFileInfo()
                .setChunkSize(100)
                .setContentType("image/jpeg")
                .setFileName("testfile.jpg")
                .setId(uuid)
                .setLength(1000L)
                .setUploadDate(123456789L);
    }
}
