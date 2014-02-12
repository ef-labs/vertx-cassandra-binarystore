package com.englishtown.vertx.cassandra.binarystore.impl;

import com.englishtown.vertx.cassandra.binarystore.FileInfo;

import java.util.Map;
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

}
