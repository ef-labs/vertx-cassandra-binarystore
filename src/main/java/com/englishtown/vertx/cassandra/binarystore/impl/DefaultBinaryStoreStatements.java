package com.englishtown.vertx.cassandra.binarystore.impl;

import com.datastax.driver.core.PreparedStatement;
import com.englishtown.vertx.cassandra.binarystore.BinaryStoreStatements;

/**
 * Default implementation of {@link com.englishtown.vertx.cassandra.binarystore.BinaryStoreStatements}
 */
public class DefaultBinaryStoreStatements implements BinaryStoreStatements {

    private String keyspace;
    private PreparedStatement storeChunk;
    private PreparedStatement storeFile;
    private PreparedStatement loadChunk;
    private PreparedStatement loadFile;

    @Override
    public String getKeyspace() {
        return keyspace;
    }

    @Override
    public BinaryStoreStatements setKeyspace(String keyspace) {
        this.keyspace = keyspace;
        return this;
    }

    @Override
    public PreparedStatement getLoadChunk() {
        return loadChunk;
    }

    @Override
    public BinaryStoreStatements setLoadChunk(PreparedStatement loadChunk) {
        this.loadChunk = loadChunk;
        return this;
    }

    @Override
    public PreparedStatement getLoadFile() {
        return loadFile;
    }

    @Override
    public BinaryStoreStatements setLoadFile(PreparedStatement loadFile) {
        this.loadFile = loadFile;
        return this;
    }

    @Override
    public PreparedStatement getStoreChunk() {
        return storeChunk;
    }

    @Override
    public BinaryStoreStatements setStoreChunk(PreparedStatement storeChunk) {
        this.storeChunk = storeChunk;
        return this;
    }

    @Override
    public PreparedStatement getStoreFile() {
        return storeFile;
    }

    @Override
    public BinaryStoreStatements setStoreFile(PreparedStatement storeFile) {
        this.storeFile = storeFile;
        return this;
    }
}
