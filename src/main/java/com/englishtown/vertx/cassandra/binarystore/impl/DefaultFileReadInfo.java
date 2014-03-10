package com.englishtown.vertx.cassandra.binarystore.impl;

import com.englishtown.vertx.cassandra.binarystore.ContentRange;
import com.englishtown.vertx.cassandra.binarystore.FileInfo;
import com.englishtown.vertx.cassandra.binarystore.FileReadInfo;

/**
 * Created by adriangonzalez on 3/10/14.
 */
public class DefaultFileReadInfo implements FileReadInfo {

    private FileInfo fileInfo;
    private ContentRange range;

    @Override
    public FileInfo getFile() {
        return fileInfo;
    }

    public DefaultFileReadInfo setFile(FileInfo fileInfo) {
        this.fileInfo = fileInfo;
        return this;
    }

    @Override
    public ContentRange getRange() {
        return range;
    }

    public DefaultFileReadInfo setRange(ContentRange range) {
        this.range = range;
        return this;
    }

}
