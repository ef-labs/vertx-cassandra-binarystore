package com.englishtown.vertx.cassandra.binarystore.impl;

import com.englishtown.promises.*;
import com.englishtown.vertx.cassandra.binarystore.BinaryStoreManager;
import com.englishtown.vertx.cassandra.binarystore.BinaryStoreWriter;
import com.englishtown.vertx.cassandra.binarystore.ChunkInfo;
import com.englishtown.vertx.cassandra.binarystore.FileInfo;
import com.google.common.util.concurrent.FutureCallback;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.streams.ReadStream;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Default implementation of {@link com.englishtown.vertx.cassandra.binarystore.BinaryStoreWriter}
 */
public class DefaultBinaryStoreWriter implements BinaryStoreWriter {

    private final BinaryStoreManager binaryStoreManager;
    private final When<Void> voidWhen = new When<>();
    public static final int DEFAULT_CHUNK_SIZE = 1024000;

    @Inject
    public DefaultBinaryStoreWriter(BinaryStoreManager binaryStoreManager) {
        this.binaryStoreManager = binaryStoreManager;
    }

    @Override
    public <T> void write(final FileInfo fileInfo, final ReadStream<T> rs, final FutureCallback<FileInfo> callback) {

        // Copy file info to a writeable version and fill in missing fields
        DefaultFileInfo writeableFileInfo = new DefaultFileInfo()
                .setId((fileInfo.getId() == null ? UUID.randomUUID() : fileInfo.getId()))
                .setFileName(fileInfo.getFileName())
                .setChunkSize((fileInfo.getChunkSize() <= 0 ? DEFAULT_CHUNK_SIZE : fileInfo.getChunkSize()))
                .setContentType((fileInfo.getContentType() == null ? getContentType(fileInfo.getFileName()) : fileInfo.getContentType()))
                .setMetadata(fileInfo.getMetadata())
                .setUploadDate((fileInfo.getUploadDate() == 0 ? System.currentTimeMillis() : fileInfo.getUploadDate()));

        innerWrite(writeableFileInfo, rs, callback);

    }

    private <T> void innerWrite(final DefaultFileInfo fileInfo, final ReadStream<T> rs, final FutureCallback<FileInfo> callback) {

        final Value<Buffer> buffer = new Value<>(new Buffer());
        final Value<Integer> num = new Value<>(0);

        final List<Promise<Void>> promises = new ArrayList<>();

        // NOTE: There is no throttling on ReadStream data.
        // This shouldn't be a problem, but could consider calling pause/resume on rs when writing chunks.
        rs.dataHandler(new Handler<Buffer>() {
            @Override
            public void handle(Buffer data) {
                handleData(data, buffer, num, fileInfo, promises);
            }
        });

        rs.endHandler(new Handler<Void>() {
            @Override
            public void handle(Void event) {

                handleEnd(buffer, num, fileInfo, promises);
                voidWhen.all(promises).then(
                        new FulfilledRunnable<List<? extends Void>>() {
                            @Override
                            public Promise<List<? extends Void>> run(List<? extends Void> voids) {
                                callback.onSuccess(fileInfo);
                                return null;
                            }
                        },
                        new RejectedRunnable<List<? extends Void>>() {
                            @Override
                            public com.englishtown.promises.Promise<List<? extends Void>> run(com.englishtown.promises.Value<List<? extends Void>> listValue) {
                                callback.onFailure(listValue.getCause());
                                return null;
                            }
                        }
                );
            }
        });

        rs.exceptionHandler(new Handler<Throwable>() {
            @Override
            public void handle(Throwable t) {
                callback.onFailure(t);
            }
        });

    }

    private void handleEnd(
            Value<Buffer> buffer,
            Value<Integer> num,
            DefaultFileInfo fileInfo,
            List<Promise<Void>> promises) {

        if (buffer.getValue().length() > 0) {
            long newLen = buffer.getValue().length() + fileInfo.getLength();
            fileInfo.setLength(newLen);

            ChunkInfo chunkInfo = new DefaultChunkInfo()
                    .setId(fileInfo.getId())
                    .setNum(num.getValue())
                    .setData(buffer.getValue().getBytes());

            final Deferred<Void> d = voidWhen.defer();
            promises.add(d.getPromise());
            binaryStoreManager.storeChunk(chunkInfo, new FutureCallback<Void>() {
                @Override
                public void onSuccess(Void result) {
                    d.getResolver().resolve((Void) null);
                }

                @Override
                public void onFailure(Throwable t) {
                    d.getResolver().reject(t);
                }
            });
        }

        final Deferred<Void> d2 = voidWhen.defer();
        promises.add(d2.getPromise());

        binaryStoreManager.storeFile(fileInfo, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void result) {
                d2.getResolver().resolve((Void) null);
            }

            @Override
            public void onFailure(Throwable t) {
                d2.getResolver().reject(t);
            }
        });

    }

    private void handleData(
            Buffer data,
            Value<Buffer> buffer,
            Value<Integer> num,
            DefaultFileInfo fileInfo,
            List<Promise<Void>> promises) {

        int newLength = buffer.getValue().length() + data.length();
        if (newLength < fileInfo.getChunkSize()) {
            // Just append
            buffer.getValue().appendBuffer(data);

        } else {
            // Have at least a full chunk
            Buffer chunk = new Buffer();
            chunk.appendBuffer(buffer.getValue());

            if (newLength == fileInfo.getChunkSize()) {
                // Exactly one chunk
                chunk.appendBuffer(data);
                buffer.setValue(new Buffer());

            } else {
                // Will have some remainder to keep in buffer
                int len = fileInfo.getChunkSize() - buffer.getValue().length();
                chunk.appendBytes(data.getBytes(0, len));

                // Add additional chunk to a new buffer
                Buffer remaining = new Buffer();
                remaining.appendBytes(data.getBytes(len, data.length()));

                buffer.setValue(remaining);
            }

            ChunkInfo chunkInfo = new DefaultChunkInfo()
                    .setId(fileInfo.getId())
                    .setNum(num.getValue())
                    .setData(chunk.getBytes());

            // Increase num of chunks and total file length
            num.setValue(num.getValue() + 1);
            long totalLen = fileInfo.getChunkSize() + fileInfo.getLength();
            fileInfo.setLength(totalLen);

            final Deferred<Void> d = voidWhen.defer();
            promises.add(d.getPromise());

            binaryStoreManager.storeChunk(chunkInfo, new FutureCallback<Void>() {
                @Override
                public void onSuccess(Void result) {
                    d.getResolver().resolve((Void) null);
                }

                @Override
                public void onFailure(Throwable t) {
                    d.getResolver().reject(t);
                }
            });
        }
    }

    private String getContentType(String name) {

        if (name == null) {
            return null;
        }

        int i = name.lastIndexOf(".");

        if (i < 0) {
            return null;
        }

        String extension = name.substring(i + 1).toLowerCase();

        switch (extension) {
            case "zip":
                return "application/zip";
            case "png":
                return "image/png";
            case "jpg":
            case "jpeg":
                return "image/jpeg";
            default:
                return null;
        }

    }

}
