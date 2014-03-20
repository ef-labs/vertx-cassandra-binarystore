package com.englishtown.vertx.cassandra.binarystore.impl;

import com.englishtown.vertx.cassandra.binarystore.FileInfo;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Created by adriangonzalez on 2/12/14.
 */
public class DefaultFileInfo implements FileInfo {

    UUID id;
    String fileName;
    String contentType;
    long length;
    int chunkSize;
    long uploadDate;

    Map<String, String> metadata;

    @Override
    public UUID getId() {
        return id;
    }

    public DefaultFileInfo setId(UUID id) {
        this.id = id;
        return this;
    }

    @Override
    public String getFileName() {
        return fileName;
    }

    public DefaultFileInfo setFileName(String fileName) {
        this.fileName = fileName;
        return this;
    }

    @Override
    public String getContentType() {
        return contentType;
    }

    public DefaultFileInfo setContentType(String contentType) {
        this.contentType = contentType;
        return this;
    }

    @Override
    public long getLength() {
        return length;
    }

    public DefaultFileInfo setLength(long length) {
        this.length = length;
        return this;
    }

    @Override
    public int getChunkSize() {
        return chunkSize;
    }

    @Override
    public int getChunkCount() {
        if (getChunkSize() <= 0) {
            return 0;
        }
        double count = (double) getLength() / getChunkSize();
        return (int) Math.ceil(count);
    }

    public DefaultFileInfo setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    @Override
    public long getUploadDate() {
        return uploadDate;
    }

    public DefaultFileInfo setUploadDate(long uploadDate) {
        this.uploadDate = uploadDate;
        return this;
    }

    @Override
    public Map<String, String> getMetadata() {
        return this.metadata;
    }

    public DefaultFileInfo setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (obj == this) return true;

        if (!(obj instanceof FileInfo)) return false;

        FileInfo other = (FileInfo) obj;

        if (!Objects.equals(this.getId(), other.getId())) return false;
        if (!Objects.equals(this.getFileName(), other.getFileName())) return false;
        if (!Objects.equals(this.getContentType(), other.getContentType())) return false;
        if (!Objects.equals(this.getLength(), other.getLength())) return false;
        if (!Objects.equals(this.getChunkSize(), other.getChunkSize())) return false;
        if (!Objects.equals(this.getUploadDate(), other.getUploadDate())) return false;

        return true;
    }
}
