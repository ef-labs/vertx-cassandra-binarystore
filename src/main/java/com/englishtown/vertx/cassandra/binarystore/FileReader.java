package com.englishtown.vertx.cassandra.binarystore;

import org.vertx.java.core.Handler;
import org.vertx.java.core.streams.ReadStream;

/**
 * A stream for {@link com.englishtown.vertx.cassandra.binarystore.FileInfo} data
 */
public interface FileReader extends ReadStream<FileReader> {

    public enum Result {
        OK,
        NOT_FOUND,
        ERROR
    }

    /**
     * Sets the handler for when {@link com.englishtown.vertx.cassandra.binarystore.FileInfo} is loaded
     *
     * @param handler
     * @return
     */
    FileReader fileHandler(Handler<FileReadInfo> handler);

    /**
     * Set a result handler. Once the stream has ended, and there is no more data to be read, this handler will be called.
     *
     * @param handler
     * @return
     */
    FileReader resultHandler(Handler<Result> handler);

}
