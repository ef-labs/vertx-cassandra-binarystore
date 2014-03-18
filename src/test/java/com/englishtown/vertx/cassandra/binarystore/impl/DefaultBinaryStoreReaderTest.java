package com.englishtown.vertx.cassandra.binarystore.impl;

import com.englishtown.vertx.cassandra.binarystore.*;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.vertx.java.core.Handler;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Container;

import java.util.UUID;

import static com.englishtown.vertx.cassandra.binarystore.FileReader.Result;
import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultBinaryStoreReaderTest {

    @Mock
    BinaryStoreManager binaryStoreManager;
    @Mock
    Container container;
    @Mock
    Handler<byte[]> dataHandler;
    @Mock
    Handler<Result> endHandler;
    @Mock
    Handler<FileReadInfo> fileHandler;
    @Mock
    Handler<Throwable> exceptionHandler;

    @Captor
    ArgumentCaptor<FutureCallback<FileInfo>> loadFileArgumentCaptor;
    @Captor
    ArgumentCaptor<FutureCallback<ChunkInfo>> loadChunkArgumentCaptor;
    @Captor
    ArgumentCaptor<DefaultFileReadInfo> fileHandlerArgumentCaptor;

    UUID uuid = UUID.fromString("739a6466-adf8-11e3-aca6-425861b86ab6");
    DefaultBinaryStoreReader dbsr;
    FileInfo fileInfo;

    @Before
    public void setUp() {
        when(container.logger()).thenReturn(mock(Logger.class));

        dbsr = new DefaultBinaryStoreReader(binaryStoreManager, container);
        fileInfo = createFileInfo();
    }

    @Test
    public void testReadWithoutRange() throws Exception {

        // When we call read and then set up the handlers for the File Reader
        FileReader fileReader = dbsr.read(uuid);
        fileReader.dataHandler(dataHandler);
        fileReader.endHandler(endHandler);
        fileReader.fileHandler(fileHandler);
        fileReader.exceptionHandler(exceptionHandler);

        // and capture the cassandra callback and call the onSuccess method on it
        verify(binaryStoreManager).loadFile(any(UUID.class), loadFileArgumentCaptor.capture());

        // and when we then call the success method on the binarystore callback
        loadFileArgumentCaptor.getValue().onSuccess(fileInfo);

        // We expect the file handler on our fileReader to be called with a defaultfilereadinfo that contains our
        // created fileinfo
        verify(fileHandler).handle(fileHandlerArgumentCaptor.capture());
        assertEquals(fileHandlerArgumentCaptor.getValue().getFile(), fileInfo);

        // When the loadChunk method is called
        verify(binaryStoreManager).loadChunk(eq(uuid), eq(0), loadChunkArgumentCaptor.capture());

        // and when we then call its success method
        byte[] data = "This is chunk 0".getBytes();
        loadChunkArgumentCaptor.getValue().onSuccess(new DefaultChunkInfo().setId(uuid).setNum(0).setData(data));

        // We expect our data handler to be called with our data
        verify(dataHandler).handle(eq(data));

        // Then we expect loadChunks to be called again
        verify(binaryStoreManager).loadChunk(eq(uuid), eq(1), loadChunkArgumentCaptor.capture());

        // And this time we say nothing is found
        loadChunkArgumentCaptor.getValue().onSuccess(null);

        // We expect our end handler to be called with the OK result
        verify(endHandler).handle(Result.OK);
    }

    @Test
    public void testReadWithChunkReadFailure() throws Exception {

        // When we call read and then set up the handlers for the File Reader
        FileReader fileReader = dbsr.read(uuid);
        fileReader.dataHandler(dataHandler);
        fileReader.endHandler(endHandler);
        fileReader.fileHandler(fileHandler);
        fileReader.exceptionHandler(exceptionHandler);

        // and capture the cassandra callback and call the onSuccess method on it
        verify(binaryStoreManager).loadFile(any(UUID.class), loadFileArgumentCaptor.capture());

        // and when we then call the success method on the binarystore callback
        loadFileArgumentCaptor.getValue().onSuccess(fileInfo);

        // The loadChunk method is called
        verify(binaryStoreManager).loadChunk(eq(uuid), eq(0), loadChunkArgumentCaptor.capture());

        // and when we then call its failure method
        loadChunkArgumentCaptor.getValue().onFailure(new Throwable("Error"));

        // We expect the end handler to be called with the error result
        verify(endHandler).handle(Result.ERROR);
    }

    @Test
    public void testReadWithFileReadFailure() throws Exception {

        // When we call read and then set up the handlers for the File Reader
        FileReader fileReader = dbsr.read(uuid);
        fileReader.dataHandler(dataHandler);
        fileReader.endHandler(endHandler);
        fileReader.fileHandler(fileHandler);
        fileReader.exceptionHandler(exceptionHandler);

        // and capture the cassandra callback
        verify(binaryStoreManager).loadFile(any(UUID.class), loadFileArgumentCaptor.capture());

        // and when we then call the failure method on the binarystore callback
        Throwable t = new Throwable("Error");
        loadFileArgumentCaptor.getValue().onFailure(t);

        // Then we expect our exception handler to be called
        verify(exceptionHandler).handle(t);
    }

    @Test
    public void testReadWithRange() throws Exception {
        // Initialise
        ContentRange range = new DefaultContentRange()
                                    .setFrom(5)
                                    .setTo(15);
        byte[] expectedRange = "fghijklmnopqrstuvwxy".getBytes();

        // When we call read and then set up the handlers for the File Reader
        FileReader fileReader = dbsr.readRange(uuid, range);
        fileReader.dataHandler(dataHandler);
        fileReader.endHandler(endHandler);
        fileReader.fileHandler(fileHandler);
        fileReader.exceptionHandler(exceptionHandler);

        // and capture the cassandra callback and call the onSuccess method on it
        verify(binaryStoreManager).loadFile(any(UUID.class), loadFileArgumentCaptor.capture());

        // and when we then call the success method on the binarystore callback
        loadFileArgumentCaptor.getValue().onSuccess(fileInfo);

        // We expect the file handler on our fileReader to be called with a defaultfilereadinfo that contains our
        // created fileinfo
        verify(fileHandler).handle(fileHandlerArgumentCaptor.capture());
        assertEquals(fileHandlerArgumentCaptor.getValue().getFile(), fileInfo);
        assertEquals(fileHandlerArgumentCaptor.getValue().getRange(), range);

        // When the loadChunk method is called
        verify(binaryStoreManager).loadChunk(eq(uuid), eq(0), loadChunkArgumentCaptor.capture());

        // and when we then call its success method
        byte[] data = "abcdefghijklmnopqrstuvwxyz".getBytes();
        loadChunkArgumentCaptor.getValue().onSuccess(new DefaultChunkInfo().setId(uuid).setNum(0).setData(data));

        // We expect our data handler to be called with our data, within the right range
        verify(dataHandler).handle(eq(expectedRange));

        // And we then expect the end handler to be called with the OK result
        verify(endHandler).handle(Result.OK);
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
