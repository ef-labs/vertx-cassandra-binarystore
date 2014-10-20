package com.englishtown.vertx.cassandra.binarystore;

import com.datastax.driver.core.PreparedStatement;
import com.englishtown.promises.Promise;

/**
 * Contains cassandra statements for loading/storing files and chunks
 */
public interface BinaryStoreStatements {

    boolean isInitialized();

    Promise<Void> init(String keyspace);

    String getKeyspace();

    PreparedStatement getLoadChunk();

    BinaryStoreStatements setLoadChunk(PreparedStatement loadChunk);

    PreparedStatement getLoadFile();

    BinaryStoreStatements setLoadFile(PreparedStatement loadFile);

    PreparedStatement getStoreChunk();

    BinaryStoreStatements setStoreChunk(PreparedStatement storeChunk);

    PreparedStatement getStoreFile();

    BinaryStoreStatements setStoreFile(PreparedStatement storeFile);

}
