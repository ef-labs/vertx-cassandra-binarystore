package com.englishtown.vertx.cassandra.binarystore;

/**
 * Created by adriangonzalez on 3/10/14.
 */
public class FileReadInfo {

    private FileInfo fileInfo;
    private ContentRange range;

    public FileInfo getFile() {
        return fileInfo;
    }

    public FileReadInfo setFile(FileInfo fileInfo) {
        this.fileInfo = fileInfo;
        return this;
    }

    public ContentRange getRange() {
        return range;
    }

    public FileReadInfo setRange(ContentRange range) {
        this.range = range;
        return this;
    }

}
