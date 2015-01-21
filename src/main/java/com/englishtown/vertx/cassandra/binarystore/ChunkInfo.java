package com.englishtown.vertx.cassandra.binarystore;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/**
 * Created by adriangonzalez on 2/12/14.
 */
public class ChunkInfo {

    private UUID id;
    private int n;
    private byte[] data;

    public UUID getId() {
        return id;
    }

    public ChunkInfo setId(UUID id) {
        this.id = id;
        return this;
    }

    public int getNum() {
        return n;
    }

    public ChunkInfo setNum(int n) {
        this.n = n;
        return this;
    }

    public byte[] getData() {
        return data;
    }

    public ChunkInfo setData(byte[] data) {
        this.data = data;
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (obj == this) return true;

        if (!(obj instanceof ChunkInfo)) return false;

        ChunkInfo other = (ChunkInfo) obj;

        if (!Objects.equals(this.getId(), other.getId())) return false;
        if (!Objects.equals(this.getNum(), other.getNum())) return false;
        if (!Arrays.equals(this.getData(), other.getData())) return false;

        return true;
    }
}
