package com.englishtown.vertx.cassandra.binarystore.impl;

import com.englishtown.promises.Promise;
import com.englishtown.vertx.cassandra.binarystore.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Container;

import java.util.UUID;
import java.util.function.Function;

import static com.englishtown.vertx.cassandra.binarystore.FileReader.Result;
import static org.junit.Assert.assertEquals;
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
    Handler<Buffer> dataHandler;
    @Mock
    Handler<Void> endHandler;
    @Mock
    Handler<Result> resultHandler;
    @Mock
    Handler<FileReadInfo> fileHandler;
    @Mock
    Handler<Throwable> exceptionHandler;
    @Mock
    Promise<FileInfo> fileInfoPromise;
    @Mock
    Promise<ChunkInfo> chunkInfoPromise;

    @Captor
    ArgumentCaptor<Function<FileInfo, Promise<FileInfo>>> fileInfoFulfilledCaptor;
    @Captor
    ArgumentCaptor<Function<Throwable, Promise<FileInfo>>> fileInfoRejectedCaptor;
    @Captor
    ArgumentCaptor<Function<ChunkInfo, Promise<ChunkInfo>>> chunkInfoFulfilledCaptor;
    @Captor
    ArgumentCaptor<Function<Throwable, Promise<ChunkInfo>>> chunkInfoRejectedCaptor;
    @Captor
    ArgumentCaptor<DefaultFileReadInfo> fileHandlerArgumentCaptor;

    private UUID uuid = UUID.fromString("739a6466-adf8-11e3-aca6-425861b86ab6");
    private DefaultBinaryStoreReader dbsr;
    private DefaultFileInfo fileInfo;

    @Before
    public void setUp() {
        fileInfo = createFileInfo();

        when(container.logger()).thenReturn(mock(Logger.class));
        when(binaryStoreManager.loadFile(any())).thenReturn(fileInfoPromise);
        when(binaryStoreManager.loadChunk(any(), anyInt())).thenReturn(chunkInfoPromise);

        when(fileInfoPromise.<FileInfo>then(any())).thenReturn(fileInfoPromise);
        when(chunkInfoPromise.<ChunkInfo>then(any())).thenReturn(chunkInfoPromise);

        dbsr = new DefaultBinaryStoreReader(binaryStoreManager, container);
    }

    @Test
    public void testReadWithoutRange() throws Exception {

        // When we call read and then set up the handlers for the File Reader
        FileReader fileReader = dbsr.read(uuid);
        fileReader.dataHandler(dataHandler);
        fileReader.resultHandler(resultHandler);
        fileReader.endHandler(endHandler);
        fileReader.fileHandler(fileHandler);
        fileReader.exceptionHandler(exceptionHandler);

        // and capture the cassandra callback and call the onSuccess method on it
        verify(binaryStoreManager).loadFile(any(UUID.class));

        // and when we then call the success method on the binarystore callback
        verify(fileInfoPromise).then(fileInfoFulfilledCaptor.capture());
        fileInfoFulfilledCaptor.getValue().apply(fileInfo);

        // We expect the file handler on our fileReader to be called with a defaultfilereadinfo that contains our
        // created fileinfo
        verify(fileHandler).handle(fileHandlerArgumentCaptor.capture());
        assertEquals(fileHandlerArgumentCaptor.getValue().getFile(), fileInfo);

        // When the loadChunk method is called
        verify(binaryStoreManager).loadChunk(eq(uuid), eq(0));

        // and when we then call its success method
        byte[] data = "This is chunk 0".getBytes();
        Buffer buffer = new Buffer(data);
        verify(chunkInfoPromise).then(chunkInfoFulfilledCaptor.capture());
        chunkInfoFulfilledCaptor.getValue().apply(new DefaultChunkInfo().setId(uuid).setNum(0).setData(data));

        // We expect our data handler to be called with our data
        verify(dataHandler).handle(eq(buffer));

        // Then we expect loadChunks to be called again
        verify(binaryStoreManager).loadChunk(eq(uuid), eq(1));

        // And this time we say nothing is found
        chunkInfoFulfilledCaptor.getValue().apply(null);

        // We expect our end handler to be called with the OK result
        verify(resultHandler).handle(Result.OK);
        verify(endHandler).handle(null);
    }

    @Test
    public void testReadWithChunkReadFailure() throws Exception {

        // When we call read and then set up the handlers for the File Reader
        FileReader fileReader = dbsr.read(uuid);
        fileReader.dataHandler(dataHandler);
        fileReader.resultHandler(resultHandler);
        fileReader.endHandler(endHandler);
        fileReader.fileHandler(fileHandler);
        fileReader.exceptionHandler(exceptionHandler);

        // and capture the cassandra callback and call the onSuccess method on it
        verify(binaryStoreManager).loadFile(any(UUID.class));

        // and when we then call the success method on the binarystore callback
        verify(fileInfoPromise).then(fileInfoFulfilledCaptor.capture());
        fileInfoFulfilledCaptor.getValue().apply(fileInfo);

        // The loadChunk method is called
        verify(binaryStoreManager).loadChunk(eq(uuid), eq(0));

        // and when we then call its failure method
        verify(chunkInfoPromise).otherwise(chunkInfoRejectedCaptor.capture());
        chunkInfoRejectedCaptor.getValue().apply(new Throwable("Error"));

        // We expect the end handler to be called with the error result
        verify(resultHandler).handle(Result.ERROR);
        verify(endHandler).handle(null);
    }

    @Test
    public void testReadWithFileReadFailure() throws Exception {

        // When we call read and then set up the handlers for the File Reader
        FileReader fileReader = dbsr.read(uuid);
        fileReader.dataHandler(dataHandler);
        fileReader.resultHandler(resultHandler);
        fileReader.endHandler(endHandler);
        fileReader.fileHandler(fileHandler);
        fileReader.exceptionHandler(exceptionHandler);

        // and capture the cassandra callback
        verify(binaryStoreManager).loadFile(any(UUID.class));

        // and when we then call the failure method on the binarystore callback
        Throwable t = new Throwable("Error");
        verify(fileInfoPromise).otherwise(fileInfoRejectedCaptor.capture());
        fileInfoRejectedCaptor.getValue().apply(t);

        // Then we expect our exception handler to be called
        verify(exceptionHandler).handle(t);
    }

    @Test
    public void testReadWithRange() throws Exception {
        // Initialise
        ContentRange range = new DefaultContentRange()
                .setFrom(5)
                .setTo(15);
        byte[] expectedRange = "fghijklmnop".getBytes();
        Buffer expectedRangeBuffer = new Buffer(expectedRange);

        // When we call read and then set up the handlers for the File Reader
        FileReader fileReader = dbsr.readRange(uuid, range);
        fileReader.dataHandler(dataHandler);
        fileReader.resultHandler(resultHandler);
        fileReader.endHandler(endHandler);
        fileReader.fileHandler(fileHandler);
        fileReader.exceptionHandler(exceptionHandler);

        // and capture the cassandra callback and call the onSuccess method on it
        verify(binaryStoreManager).loadFile(any(UUID.class));

        // and when we then call the success method on the binarystore callback
        verify(fileInfoPromise).then(fileInfoFulfilledCaptor.capture());
        fileInfoFulfilledCaptor.getValue().apply(fileInfo);

        // We expect the file handler on our fileReader to be called with a defaultfilereadinfo that contains our
        // created fileinfo
        verify(fileHandler).handle(fileHandlerArgumentCaptor.capture());
        assertEquals(fileHandlerArgumentCaptor.getValue().getFile(), fileInfo);
        assertEquals(fileHandlerArgumentCaptor.getValue().getRange(), range);

        // When the loadChunk method is called
        verify(binaryStoreManager).loadChunk(eq(uuid), eq(0));

        // and when we then call its success method
        byte[] data = "abcdefghijklmnopqrstuvwxyz".getBytes();
        verify(chunkInfoPromise).then(chunkInfoFulfilledCaptor.capture());
        chunkInfoFulfilledCaptor.getValue().apply(new DefaultChunkInfo().setId(uuid).setNum(0).setData(data));

        // We expect our data handler to be called with our data, within the right range
        verify(dataHandler).handle(eq(expectedRangeBuffer));

        // And we then expect the end handler to be called with the OK result
        verify(resultHandler).handle(Result.OK);
        verify(endHandler).handle(null);
    }

    @Test
    public void testRangeReadOfFinalByte() throws Exception {
        // Initialise
        ContentRange range = new DefaultContentRange()
                .setFrom(25)
                .setTo(25);
        byte[] expectedRange = "z".getBytes();
        Buffer expectedRangeBuffer = new Buffer(expectedRange);

        // When we call read and then set up the handlers for the File Reader
        FileReader fileReader = dbsr.readRange(uuid, range);
        fileReader.dataHandler(dataHandler);
        fileReader.resultHandler(resultHandler);
        fileReader.endHandler(endHandler);
        fileReader.fileHandler(fileHandler);
        fileReader.exceptionHandler(exceptionHandler);

        // and capture the cassandra callback and call the onSuccess method on it
        verify(binaryStoreManager).loadFile(any(UUID.class));

        // and when we then call the success method on the binarystore callback
        verify(fileInfoPromise).then(fileInfoFulfilledCaptor.capture());
        fileInfoFulfilledCaptor.getValue().apply(fileInfo);

        // We expect the file handler on our fileReader to be called with a defaultfilereadinfo that contains our
        // created fileinfo
        verify(fileHandler).handle(fileHandlerArgumentCaptor.capture());
        assertEquals(fileHandlerArgumentCaptor.getValue().getFile(), fileInfo);
        assertEquals(fileHandlerArgumentCaptor.getValue().getRange(), range);

        // When the loadChunk method is called
        verify(binaryStoreManager).loadChunk(eq(uuid), eq(0));

        // and when we then call its success method
        byte[] data = "abcdefghijklmnopqrstuvwxyz".getBytes();
        verify(chunkInfoPromise).then(chunkInfoFulfilledCaptor.capture());
        chunkInfoFulfilledCaptor.getValue().apply(new DefaultChunkInfo().setId(uuid).setNum(0).setData(data));

        // We expect our data handler to be called with our data, within the right range
        verify(dataHandler).handle(eq(expectedRangeBuffer));

        // And we then expect the end handler to be called with the OK result
        verify(resultHandler).handle(Result.OK);
        verify(endHandler).handle(null);
    }

    @Test
    public void testRangeReadOfFirstByte() throws Exception {
        // Initialise
        ContentRange range = new DefaultContentRange()
                .setFrom(0)
                .setTo(0);
        byte[] expectedRange = "a".getBytes();
        Buffer expectedRangeBuffer = new Buffer(expectedRange);

        // When we call read and then set up the handlers for the File Reader
        FileReader fileReader = dbsr.readRange(uuid, range);
        fileReader.dataHandler(dataHandler);
        fileReader.resultHandler(resultHandler);
        fileReader.endHandler(endHandler);
        fileReader.fileHandler(fileHandler);
        fileReader.exceptionHandler(exceptionHandler);

        // and capture the cassandra callback and call the onSuccess method on it
        verify(binaryStoreManager).loadFile(any(UUID.class));

        // and when we then call the success method on the binarystore callback
        verify(fileInfoPromise).then(fileInfoFulfilledCaptor.capture());
        fileInfoFulfilledCaptor.getValue().apply(fileInfo);

        // We expect the file handler on our fileReader to be called with a defaultfilereadinfo that contains our
        // created fileinfo
        verify(fileHandler).handle(fileHandlerArgumentCaptor.capture());
        assertEquals(fileHandlerArgumentCaptor.getValue().getFile(), fileInfo);
        assertEquals(fileHandlerArgumentCaptor.getValue().getRange(), range);

        // When the loadChunk method is called
        verify(binaryStoreManager).loadChunk(eq(uuid), eq(0));

        // and when we then call its success method
        byte[] data = "abcdefghijklmnopqrstuvwxyz".getBytes();
        verify(chunkInfoPromise).then(chunkInfoFulfilledCaptor.capture());
        chunkInfoFulfilledCaptor.getValue().apply(new DefaultChunkInfo().setId(uuid).setNum(0).setData(data));

        // We expect our data handler to be called with our data, within the right range
        verify(dataHandler).handle(eq(expectedRangeBuffer));

        // And we then expect the end handler to be called with the OK result
        verify(resultHandler).handle(Result.OK);
        verify(endHandler).handle(null);
    }

    @Test
    public void testRangeReadOverMultipleChunks() throws Exception {
        // Initialise
        FileInfo multiChunkFile = new DefaultFileInfo()
                .setChunkSize(26)
                .setContentType("image/jpeg")
                .setFileName("testfile.jpg")
                .setId(uuid)
                .setLength(52L)
                .setUploadDate(123456789L);

        ContentRange range = new DefaultContentRange()
                .setFrom(20)
                .setTo(30);
        byte[] expectedRangeForChunk1 = "uvwxyz".getBytes();
        Buffer expectedRangeBufferForChunk1 = new Buffer(expectedRangeForChunk1);
        byte[] expectedRangeForChunk2 = "abcde".getBytes();
        Buffer expectedRangeBufferForChunk2 = new Buffer(expectedRangeForChunk2);

        // When we call read and then set up the handlers for the File Reader
        FileReader fileReader = dbsr.readRange(uuid, range);
        fileReader.dataHandler(dataHandler);
        fileReader.resultHandler(resultHandler);
        fileReader.endHandler(endHandler);
        fileReader.fileHandler(fileHandler);
        fileReader.exceptionHandler(exceptionHandler);

        // and capture the cassandra callback and call the onSuccess method on it
        verify(binaryStoreManager).loadFile(any(UUID.class));

        // and when we then call the success method on the binarystore callback
        verify(fileInfoPromise).then(fileInfoFulfilledCaptor.capture());
        fileInfoFulfilledCaptor.getValue().apply(multiChunkFile);

        // We expect the file handler on our fileReader to be called with a defaultfilereadinfo that contains our
        // created fileinfo
        verify(fileHandler).handle(fileHandlerArgumentCaptor.capture());
        assertEquals(fileHandlerArgumentCaptor.getValue().getFile(), multiChunkFile);
        assertEquals(fileHandlerArgumentCaptor.getValue().getRange(), range);

        // When the loadChunk method is called
        verify(binaryStoreManager).loadChunk(eq(uuid), eq(0));

        // and when we then call its success method
        byte[] data = "abcdefghijklmnopqrstuvwxyz".getBytes();
        verify(chunkInfoPromise).then(chunkInfoFulfilledCaptor.capture());
        chunkInfoFulfilledCaptor.getValue().apply(new DefaultChunkInfo().setId(uuid).setNum(0).setData(data));

        // We expect our data handler to be called with our data, within the right range
        verify(dataHandler).handle(eq(expectedRangeBufferForChunk1));

        // When the loadChunk method is called
        verify(binaryStoreManager).loadChunk(eq(uuid), eq(1));

        // and when we then call its success method
        verify(chunkInfoPromise, times(2)).then(chunkInfoFulfilledCaptor.capture());
        chunkInfoFulfilledCaptor.getValue().apply(new DefaultChunkInfo().setId(uuid).setNum(1).setData(data));

        // We expect our data handler to be called with our data, within the right range
        verify(dataHandler).handle(eq(expectedRangeBufferForChunk2));

        // And we then expect the end handler to be called with the OK result
        verify(resultHandler).handle(Result.OK);
        verify(endHandler).handle(null);
    }

    private DefaultFileInfo createFileInfo() {
        return new DefaultFileInfo()
                .setChunkSize(100)
                .setContentType("image/jpeg")
                .setFileName("testfile.jpg")
                .setId(uuid)
                .setLength(1000L)
                .setUploadDate(123456789L);
    }
}
