package com.englishtown.vertx.cassandra.binarystore;

import org.vertx.java.core.Handler;
import org.vertx.java.core.streams.ExceptionSupport;

/**
 * A stream for {@link com.englishtown.vertx.cassandra.binarystore.FileInfo} data
 */
public interface FileReader extends ExceptionSupport<FileReader> {

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
     * Sets the handler for when data is read from the binary store
     *
     * @param handler
     * @return
     */
    FileReader dataHandler(Handler<byte[]> handler);

    /**
     * Sets the handler for when all the data has been read
     *
     * @param handler
     * @return
     */
    FileReader endHandler(Handler<Result> handler);

}
