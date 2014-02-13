package com.englishtown.vertx.cassandra.binarystore;

import java.util.Map;
import java.util.UUID;

/**
 * File info
 */
public interface FileInfo {

    UUID getId();

    String getFileName();

    String getContentType();

    long getLength();

    int getChunkSize();

    int getChunkCount();

    long getUploadDate();

    Map<String, String> getMetadata();

}
