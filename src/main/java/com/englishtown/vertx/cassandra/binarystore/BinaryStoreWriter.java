package com.englishtown.vertx.cassandra.binarystore;

import com.google.common.util.concurrent.FutureCallback;
import org.vertx.java.core.streams.ReadStream;

/**
 * Created by adriangonzalez on 3/7/14.
 */
public interface BinaryStoreWriter {

    <T> void write(FileInfo fileInfo, ReadStream<T> rs, FutureCallback<FileInfo> callback);

}
