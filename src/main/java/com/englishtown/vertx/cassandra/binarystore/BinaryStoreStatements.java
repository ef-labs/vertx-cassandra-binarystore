package com.englishtown.vertx.cassandra.binarystore;

import com.datastax.driver.core.PreparedStatement;

/**
 * Contains cassandra statements for loading/storing files and chunks
 */
public interface BinaryStoreStatements {

    String getKeyspace();

    BinaryStoreStatements setKeyspace(String keyspace);

    PreparedStatement getLoadChunk();

    BinaryStoreStatements setLoadChunk(PreparedStatement loadChunk);

    PreparedStatement getLoadFile();

    BinaryStoreStatements setLoadFile(PreparedStatement loadFile);

    PreparedStatement getStoreChunk();

    BinaryStoreStatements setStoreChunk(PreparedStatement storeChunk);

    PreparedStatement getStoreFile();

    BinaryStoreStatements setStoreFile(PreparedStatement storeFile);

}
