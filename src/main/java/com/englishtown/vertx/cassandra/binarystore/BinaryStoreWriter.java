package com.englishtown.vertx.cassandra.binarystore;

import com.englishtown.promises.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

/**
 * Created by adriangonzalez on 3/7/14.
 */
public interface BinaryStoreWriter {

    Promise<FileInfo> write(FileInfo fileInfo, ReadStream<Buffer> rs);

}
