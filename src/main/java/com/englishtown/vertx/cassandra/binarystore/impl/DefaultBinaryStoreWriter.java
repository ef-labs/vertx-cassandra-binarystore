package com.englishtown.vertx.cassandra.binarystore.impl;

import com.englishtown.promises.Deferred;
import com.englishtown.promises.Promise;
import com.englishtown.promises.When;
import com.englishtown.vertx.cassandra.binarystore.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Default implementation of {@link com.englishtown.vertx.cassandra.binarystore.BinaryStoreWriter}
 */
public class DefaultBinaryStoreWriter implements BinaryStoreWriter {

    private final BinaryStoreManager binaryStoreManager;
    private final When when;
    public static final int DEFAULT_CHUNK_SIZE = 1024000;

    @Inject
    public DefaultBinaryStoreWriter(BinaryStoreManager binaryStoreManager, When when) {
        this.binaryStoreManager = binaryStoreManager;
        this.when = when;
    }

    @Override
    public Promise<FileInfo> write(final FileInfo fileInfo, final ReadStream<Buffer> rs) {

        // Copy file info to a writeable version and fill in missing fields
        FileInfo writeableFileInfo = new FileInfo()
                .setId((fileInfo.getId() == null ? UUID.randomUUID() : fileInfo.getId()))
                .setFileName(fileInfo.getFileName())
                .setChunkSize((fileInfo.getChunkSize() <= 0 ? DEFAULT_CHUNK_SIZE : fileInfo.getChunkSize()))
                .setContentType((fileInfo.getContentType() == null ? getContentType(fileInfo.getFileName()) : fileInfo.getContentType()))
                .setMetadata(fileInfo.getMetadata())
                .setUploadDate((fileInfo.getUploadDate() == 0 ? System.currentTimeMillis() : fileInfo.getUploadDate()));

        return innerWrite(writeableFileInfo, rs);

    }

    private <T> Promise<FileInfo> innerWrite(final FileInfo fileInfo, final ReadStream<Buffer> rs) {

        WriteInfo info = new WriteInfo();
        List<Promise<Void>> promises = new ArrayList<>();
        Deferred<FileInfo> d = when.defer();

        // NOTE: There is no throttling on ReadStream data.
        // This shouldn't be a problem, but could consider calling pause/resume on rs when writing chunks.
        rs.handler(data -> handleData(data, info, fileInfo, promises));

        rs.endHandler(event -> {
            handleEnd(info.buffer, info.num, fileInfo, promises);
            d.resolve(when.all(promises).then(voids -> when.resolve(fileInfo)));
        });

        rs.exceptionHandler(t -> d.reject(t));

        return d.getPromise();
    }

    private void handleEnd(
            Buffer buffer,
            int num,
            FileInfo fileInfo,
            List<Promise<Void>> promises) {

        if (buffer.length() > 0) {
            long newLen = buffer.length() + fileInfo.getLength();
            fileInfo.setLength(newLen);

            ChunkInfo chunkInfo = new ChunkInfo()
                    .setId(fileInfo.getId())
                    .setNum(num)
                    .setData(buffer.getBytes());

            promises.add(binaryStoreManager.storeChunk(chunkInfo));
        }

        promises.add(binaryStoreManager.storeFile(fileInfo));

    }

    private void handleData(
            Buffer data,
            WriteInfo info,
            FileInfo fileInfo,
            List<Promise<Void>> promises) {

        int newLength = info.buffer.length() + data.length();
        if (newLength < fileInfo.getChunkSize()) {
            // Just append
            info.buffer.appendBuffer(data);

        } else {
            // Have at least a full chunk
            Buffer chunk = Buffer.buffer();
            chunk.appendBuffer(info.buffer);

            if (newLength == fileInfo.getChunkSize()) {
                // Exactly one chunk
                chunk.appendBuffer(data);
                info.buffer = Buffer.buffer();

            } else {
                // Will have some remainder to keep in buffer
                int len = fileInfo.getChunkSize() - info.buffer.length();
                chunk.appendBytes(data.getBytes(0, len));

                // Add additional chunk to a new buffer
                Buffer remaining = Buffer.buffer();
                remaining.appendBytes(data.getBytes(len, data.length()));

                info.buffer = remaining;
            }

            ChunkInfo chunkInfo = new ChunkInfo()
                    .setId(fileInfo.getId())
                    .setNum(info.num)
                    .setData(chunk.getBytes());

            // Increase num of chunks and total file length
            info.num += 1;
            long totalLen = fileInfo.getChunkSize() + fileInfo.getLength();
            fileInfo.setLength(totalLen);

            promises.add(binaryStoreManager.storeChunk(chunkInfo));

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
            case "mp3":
                return "audio/mpeg";
            case "m4a":
                return "audio/mp4";
            case "mov":
                return "video/quicktime";
            case "mp4":
                return "video/mp4";
            case "ogg":
                return "video/ogg";
            case "webm":
                return "video/webm";
            default:
                return null;
        }

    }

    private static class WriteInfo {
        Buffer buffer = Buffer.buffer();
        int num = 0;
    }

}
