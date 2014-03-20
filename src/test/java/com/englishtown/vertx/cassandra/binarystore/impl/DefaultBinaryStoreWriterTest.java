package com.englishtown.vertx.cassandra.binarystore.impl;

import com.englishtown.vertx.cassandra.binarystore.BinaryStoreManager;
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
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.file.AsyncFile;
import org.vertx.java.core.streams.ReadStream;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultBinaryStoreWriterTest {

    @Mock
    BinaryStoreManager binaryStoreManager;
    @Mock
    ReadStream<AsyncFile> readStream;
    @Mock
    FutureCallback<FileInfo> callback;

    @Captor
    ArgumentCaptor<FutureCallback<Void>> chunkFutureCallbackArgumentCaptor;
    @Captor
    ArgumentCaptor<FutureCallback<Void>> fileFutureCallbackArgumentCaptor;
    @Captor
    ArgumentCaptor<Handler<Buffer>> dataHandlerCaptor;
    @Captor
    ArgumentCaptor<Handler<Void>> endHandlerCaptor;
    @Captor
    ArgumentCaptor<Handler<Throwable>> exceptionHandlerCaptor;
    @Captor
    ArgumentCaptor<FileInfo> fileInfoArgumentCaptor;

    UUID uuid = UUID.fromString("739a6466-adf8-11e3-aca6-425861b86ab6");
    DefaultBinaryStoreWriter dbsw;
    DefaultFileInfo fileInfo;
    String[] names = {"test.zip", "test.png", "test.jpg", "test.jpeg", "test.mp3", "test.m4a", "test.mov", "test.mp4",
            "test.ogg", "test.webm"};
    String[] types = {"application/zip", "image/png", "image/jpeg", "image/jpeg", "audio/mpeg", "audio/mp4",
            "video/quicktime", "video/mp4", "video/ogg", "video/webm"};

    @Before
    public void setUp() throws Exception {
        dbsw = new DefaultBinaryStoreWriter(binaryStoreManager);
        fileInfo = createFileInfo();

//        verify(readStream).exceptionHandler(exceptionHandlerCaptor.capture());
    }

    @Test
    public void testWritingWithSingleChunk() throws Exception {
        // Set up our data
        Buffer buffer = new Buffer().appendBytes("This is just a small amount of data".getBytes());
        ChunkInfo expectedChunkInfo = new DefaultChunkInfo().setId(uuid).setNum(0).setData(buffer.getBytes());
        fileInfo.setLength(buffer.length());

        // When we call the write method
        dbsw.write(fileInfo, readStream, callback);
        verify(readStream).dataHandler(dataHandlerCaptor.capture());
        verify(readStream).endHandler(endHandlerCaptor.capture());

        // and call the data handler set on the read stream
        dataHandlerCaptor.getValue().handle(buffer);

        // Then we expect there to be no interactions with the binary store manager
        verifyZeroInteractions(binaryStoreManager);

        // When we call the end handler
        endHandlerCaptor.getValue().handle(null);

        // We expect the binary store manager to be called to write our chunk and then our file
        verify(binaryStoreManager).storeChunk(eq(expectedChunkInfo), chunkFutureCallbackArgumentCaptor.capture());
        verify(binaryStoreManager).storeFile(eq(fileInfo), fileFutureCallbackArgumentCaptor.capture());

        // When we call the success method on both of those requests
        chunkFutureCallbackArgumentCaptor.getValue().onSuccess(null);
        fileFutureCallbackArgumentCaptor.getValue().onSuccess(null);

        // Then we expect our main callback to have its success method called with our file info object
        verify(callback).onSuccess(eq(fileInfo));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testWritingWithMultipleChunks() throws Exception {

        // Build up buffer that makes two chunks
        Buffer buffer = new Buffer();
        for (int i = 0; i < 150; i++) {
            buffer.appendByte((byte) i);
        }

        // Set up our expected chunkInfo
        ChunkInfo expectedChunkInfo = new DefaultChunkInfo().setId(uuid).setNum(0).setData(buffer.getBytes(0, 100));
        ChunkInfo expectedChunkInfo2 = new DefaultChunkInfo().setId(uuid).setNum(1).setData(buffer.getBytes(100, buffer.length()));

        // Then call the write method
        dbsw.write(fileInfo, readStream, callback);
        verify(readStream).dataHandler(dataHandlerCaptor.capture());
        verify(readStream).endHandler(endHandlerCaptor.capture());

        // and call the data handler set on the read stream
        dataHandlerCaptor.getValue().handle(buffer);

        // We then expect binary store manager to be called with the expected chunk info
        verify(binaryStoreManager).storeChunk(eq(expectedChunkInfo), chunkFutureCallbackArgumentCaptor.capture());

        // When we call the success method on the binary store callback
        chunkFutureCallbackArgumentCaptor.getValue().onSuccess(null);

        // and we call the end handler
        endHandlerCaptor.getValue().handle(null);

        // We expect binary store manager to be called to store the final chunk and to store the file
        verify(binaryStoreManager).storeChunk(eq(expectedChunkInfo2), chunkFutureCallbackArgumentCaptor.capture());
        verify(binaryStoreManager).storeFile(eq(fileInfo), fileFutureCallbackArgumentCaptor.capture());

        // When we call the success method on both of those requests
        chunkFutureCallbackArgumentCaptor.getValue().onSuccess(null);
        fileFutureCallbackArgumentCaptor.getValue().onSuccess(null);

        // Then we expect our main callback to have its success method called with our file info object
        verify(callback).onSuccess(eq(fileInfo));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testContentTypes() throws Exception {

        // For each valid type of name
        for (int i = 0; i < names.length; i++) {
            BinaryStoreManager binaryStoreManager = mock(BinaryStoreManager.class);

            dbsw = new DefaultBinaryStoreWriter(binaryStoreManager);
            fileInfo = createFileInfo();
            fileInfo.setFileName(names[i]);
            fileInfo.setContentType(null);

            ReadStream<AsyncFile> readStream = mock(ReadStream.class);
            FutureCallback<FileInfo> callback = mock(FutureCallback.class);
            ArgumentCaptor<Handler> endHandlerCaptor = ArgumentCaptor.forClass(Handler.class);
            ArgumentCaptor<FileInfo> fileInfoArgumentCaptor = ArgumentCaptor.forClass(FileInfo.class);
            ArgumentCaptor<FutureCallback> fileFutureCallbackArgumentCaptor = ArgumentCaptor.forClass(FutureCallback.class);

            // We try and write and capture the end handler
            dbsw.write(fileInfo, readStream, callback);
            verify(readStream).endHandler(endHandlerCaptor.capture());

            // We then call the end handler
            endHandlerCaptor.getValue().handle(null);

            // and expect binarystoremanager to have the storefile method called with a fileinfo object that has the
            // correct contentype on it.
            verify(binaryStoreManager).storeFile(fileInfoArgumentCaptor.capture(), fileFutureCallbackArgumentCaptor.capture());
            assertEquals(types[i], fileInfoArgumentCaptor.getValue().getContentType());
        }
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
