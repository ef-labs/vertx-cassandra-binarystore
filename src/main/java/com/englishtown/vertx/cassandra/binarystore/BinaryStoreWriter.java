package com.englishtown.vertx.cassandra.binarystore;

import com.englishtown.promises.Promise;
import org.vertx.java.core.streams.ReadStream;

/**
 * Created by adriangonzalez on 3/7/14.
 */
public interface BinaryStoreWriter {

    <T> Promise<FileInfo> write(FileInfo fileInfo, ReadStream<T> rs);

}
