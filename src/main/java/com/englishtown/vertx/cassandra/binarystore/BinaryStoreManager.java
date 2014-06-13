package com.englishtown.vertx.cassandra.binarystore;

import com.google.common.util.concurrent.FutureCallback;

import java.util.UUID;

/**
 * Created by adriangonzalez on 2/12/14.
 */
public interface BinaryStoreManager {

    void storeFile(FileInfo fileInfo, FutureCallback<Void> callback);

    void storeChunk(ChunkInfo chunkInfo, FutureCallback<Void> callback);

    void loadFile(UUID id, FutureCallback<FileInfo> callback);

    void loadChunk(UUID id, int n, FutureCallback<ChunkInfo> callback);

}
