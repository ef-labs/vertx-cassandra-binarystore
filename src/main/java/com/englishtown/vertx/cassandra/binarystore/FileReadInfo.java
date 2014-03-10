package com.englishtown.vertx.cassandra.binarystore;

/**
 * Created by adriangonzalez on 3/10/14.
 */
public interface FileReadInfo {

    FileInfo getFile();

    ContentRange getRange();

}
