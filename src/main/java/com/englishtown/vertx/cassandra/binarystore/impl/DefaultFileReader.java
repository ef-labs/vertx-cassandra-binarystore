package com.englishtown.vertx.cassandra.binarystore.impl;

import com.englishtown.vertx.cassandra.binarystore.FileReadInfo;
import com.englishtown.vertx.cassandra.binarystore.FileReader;
import org.vertx.java.core.Handler;

/**
 * Default implementation of {@link com.englishtown.vertx.cassandra.binarystore.FileReader}
 */
class DefaultFileReader implements FileReader {

    private Handler<FileReadInfo> fileHandler;
    private Handler<byte[]> dataHandler;
    private Handler<Result> endHandler;
    private Handler<Throwable> exceptionHandler;

    @Override
    public DefaultFileReader fileHandler(Handler<FileReadInfo> handler) {
        fileHandler = handler;
        return this;
    }

    @Override
    public DefaultFileReader dataHandler(Handler<byte[]> handler) {
        dataHandler = handler;
        return this;
    }

    @Override
    public DefaultFileReader endHandler(Handler<Result> handler) {
        endHandler = handler;
        return this;
    }

    @Override
    public DefaultFileReader exceptionHandler(Handler<Throwable> handler) {
        exceptionHandler = handler;
        return this;
    }

    public void handleFile(FileReadInfo fileInfo) {
        if (fileHandler != null) {
            fileHandler.handle(fileInfo);
        }
    }

    public void handleData(byte[] data) {
        if (dataHandler != null) {
            dataHandler.handle(data);
        }
    }

    public void handleEnd(Result result) {
        if (endHandler != null) {
            endHandler.handle(result);
        }
    }

    public void handleException(Throwable t) {
        if (exceptionHandler != null) {
            exceptionHandler.handle(t);
        }
    }

}
