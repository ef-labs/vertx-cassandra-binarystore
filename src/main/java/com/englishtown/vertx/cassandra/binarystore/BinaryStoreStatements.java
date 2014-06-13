package com.englishtown.vertx.cassandra.binarystore;

import com.datastax.driver.core.PreparedStatement;
import com.google.common.util.concurrent.FutureCallback;

/**
 * Contains cassandra statements for loading/storing files and chunks
 */
public interface BinaryStoreStatements {

    boolean isInitialized();

    void init(String keyspace, FutureCallback<Void> callback);

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
