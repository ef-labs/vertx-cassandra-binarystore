package com.englishtown.vertx.cassandra.binarystore;

import com.englishtown.promises.Promise;

import java.util.UUID;

/**
 * Created by adriangonzalez on 2/12/14.
 */
public interface BinaryStoreManager {

    Promise<Void> storeFile(FileInfo fileInfo);

    Promise<Void> storeChunk(ChunkInfo chunkInfo);

    Promise<FileInfo> loadFile(UUID id);

    Promise<ChunkInfo> loadChunk(UUID id, int n);

}
