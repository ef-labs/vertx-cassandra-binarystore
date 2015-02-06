package com.englishtown.vertx.cassandra.binarystore.impl;

import com.englishtown.promises.HandlerState;
import com.englishtown.promises.Promise;
import com.englishtown.promises.When;
import com.englishtown.promises.WhenFactory;
import com.englishtown.vertx.cassandra.binarystore.BinaryStoreManager;
import com.englishtown.vertx.cassandra.binarystore.ChunkInfo;
import com.englishtown.vertx.cassandra.binarystore.FileInfo;
import com.google.common.util.concurrent.FutureCallback;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

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
    ReadStream<Buffer> readStream;

    @Captor
    ArgumentCaptor<Handler<Buffer>> dataHandlerCaptor;
    @Captor
    ArgumentCaptor<Handler<Void>> endHandlerCaptor;
    @Captor
    ArgumentCaptor<Handler<Throwable>> exceptionHandlerCaptor;
    @Captor
    ArgumentCaptor<FileInfo> fileInfoArgumentCaptor;

    When when;
    UUID uuid = UUID.fromString("739a6466-adf8-11e3-aca6-425861b86ab6");
    DefaultBinaryStoreWriter dbsw;
    FileInfo fileInfo;
    String[] names = {"test.zip", "test.png", "test.jpg", "test.jpeg", "test.mp3", "test.m4a", "test.mov", "test.mp4",
            "test.ogg", "test.webm"};
    String[] types = {"application/zip", "image/png", "image/jpeg", "image/jpeg", "audio/mpeg", "audio/mp4",
            "video/quicktime", "video/mp4", "video/ogg", "video/webm"};

    @Before
    public void setUp() throws Exception {
        when = WhenFactory.createSync();
        dbsw = new DefaultBinaryStoreWriter(binaryStoreManager, when);
        fileInfo = createFileInfo();

//        verify(readStream).exceptionHandler(exceptionHandlerCaptor.capture());
    }

    @Test
    public void testWritingWithSingleChunk() throws Exception {
        // Set up our data
        Buffer buffer = Buffer.buffer("This is just a small amount of data");
        ChunkInfo expectedChunkInfo = new ChunkInfo().setId(uuid).setNum(0).setData(buffer.getBytes());
        fileInfo.setLength(buffer.length());

        // When we call the write method
        Promise<FileInfo> p = dbsw.write(fileInfo, readStream);
        verify(readStream).handler(dataHandlerCaptor.capture());
        verify(readStream).endHandler(endHandlerCaptor.capture());

        // and call the data handler set on the read stream
        dataHandlerCaptor.getValue().handle(buffer);

        // Then we expect there to be no interactions with the binary store manager
        verifyZeroInteractions(binaryStoreManager);

        // When we call the end handler
        endHandlerCaptor.getValue().handle(null);

        // We expect the binary store manager to be called to write our chunk and then our file
        verify(binaryStoreManager).storeChunk(eq(expectedChunkInfo));
        verify(binaryStoreManager).storeFile(eq(fileInfo));

        // Then we expect our main callback to have its success method called with our file info object
        assertEquals(HandlerState.FULFILLED, p.inspect().getState());
        assertEquals(fileInfo, p.inspect().getValue());

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testWritingWithMultipleChunks() throws Exception {

        // Build up buffer that makes two chunks
        Buffer buffer = Buffer.buffer();
        for (int i = 0; i < 150; i++) {
            buffer.appendByte((byte) i);
        }

        // Set up our expected chunkInfo
        ChunkInfo expectedChunkInfo = new ChunkInfo().setId(uuid).setNum(0).setData(buffer.getBytes(0, 100));
        ChunkInfo expectedChunkInfo2 = new ChunkInfo().setId(uuid).setNum(1).setData(buffer.getBytes(100, buffer.length()));

        // Then call the write method
        Promise<FileInfo> p = dbsw.write(fileInfo, readStream);
        verify(readStream).handler(dataHandlerCaptor.capture());
        verify(readStream).endHandler(endHandlerCaptor.capture());

        // and call the data handler set on the read stream
        dataHandlerCaptor.getValue().handle(buffer);

        // We then expect binary store manager to be called with the expected chunk info
        verify(binaryStoreManager).storeChunk(eq(expectedChunkInfo));

        // and we call the end handler
        endHandlerCaptor.getValue().handle(null);

        // We expect binary store manager to be called to store the final chunk and to store the file
        verify(binaryStoreManager).storeChunk(eq(expectedChunkInfo2));
        verify(binaryStoreManager).storeFile(eq(fileInfo));

        // Then we expect our main callback to have its success method called with our file info object
        assertEquals(HandlerState.FULFILLED, p.inspect().getState());
        assertEquals(fileInfo, p.inspect().getValue());

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testContentTypes() throws Exception {

        // For each valid type of name
        for (int i = 0; i < names.length; i++) {
            BinaryStoreManager binaryStoreManager = mock(BinaryStoreManager.class);

            dbsw = new DefaultBinaryStoreWriter(binaryStoreManager, when);
            fileInfo = createFileInfo();
            fileInfo.setFileName(names[i]);
            fileInfo.setContentType(null);

            ReadStream<Buffer> readStream = mock(ReadStream.class);
            FutureCallback<FileInfo> callback = mock(FutureCallback.class);
            ArgumentCaptor<Handler> endHandlerCaptor = ArgumentCaptor.forClass(Handler.class);
            ArgumentCaptor<FileInfo> fileInfoArgumentCaptor = ArgumentCaptor.forClass(FileInfo.class);

            // We try and write and capture the end handler
            Promise<FileInfo> p = dbsw.write(fileInfo, readStream);
            verify(readStream).endHandler(endHandlerCaptor.capture());

            // We then call the end handler
            endHandlerCaptor.getValue().handle(null);

            // and expect binarystoremanager to have the storefile method called with a fileinfo object that has the
            // correct contentype on it.
            verify(binaryStoreManager).storeFile(fileInfoArgumentCaptor.capture());
            assertEquals(types[i], fileInfoArgumentCaptor.getValue().getContentType());
        }
    }


    private FileInfo createFileInfo() {
        return new FileInfo()
                .setChunkSize(100)
                .setContentType("image/jpeg")
                .setFileName("testfile.jpg")
                .setId(uuid)
                .setLength(150L)
                .setUploadDate(123456789L);
    }
}
