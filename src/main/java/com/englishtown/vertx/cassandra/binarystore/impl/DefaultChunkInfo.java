package com.englishtown.vertx.cassandra.binarystore.impl;

import com.englishtown.vertx.cassandra.binarystore.ChunkInfo;

import java.util.UUID;

/**
 * Created by adriangonzalez on 2/12/14.
 */
public class DefaultChunkInfo implements ChunkInfo {

    private UUID id;
    private int n;
    private byte[] data;

    @Override
    public UUID getId() {
        return id;
    }

    public DefaultChunkInfo setId(UUID id) {
        this.id = id;
        return this;
    }

    @Override
    public int getNum() {
        return n;
    }

    public DefaultChunkInfo setNum(int n) {
        this.n = n;
        return this;
    }

    @Override
    public byte[] getData() {
        return data;
    }

    public DefaultChunkInfo setData(byte[] data) {
        this.data = data;
        return this;
    }

}
