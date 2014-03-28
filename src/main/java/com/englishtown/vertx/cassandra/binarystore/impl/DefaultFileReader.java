package com.englishtown.vertx.cassandra.binarystore.impl;

import com.englishtown.vertx.cassandra.binarystore.FileReadInfo;
import com.englishtown.vertx.cassandra.binarystore.FileReader;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;

/**
 * Default implementation of {@link com.englishtown.vertx.cassandra.binarystore.FileReader}
 */
class DefaultFileReader implements FileReader {

    private Handler<FileReadInfo> fileHandler;
    private Handler<Buffer> dataHandler;
    private Handler<Void> endHandler;
    private Handler<Result> resultHandler;
    private Handler<Throwable> exceptionHandler;

    private boolean paused;
    private Handler<Void> resumeHandler;

    @Override
    public DefaultFileReader fileHandler(Handler<FileReadInfo> handler) {
        fileHandler = handler;
        return this;
    }

    @Override
    public DefaultFileReader dataHandler(Handler<Buffer> handler) {
        dataHandler = handler;
        return this;
    }

    /**
     * Pause the {@code ReadSupport}. While it's paused, no data will be sent to the {@code dataHandler}
     */
    @Override
    public DefaultFileReader pause() {
        paused = true;
        return this;
    }

    /**
     * Resume reading. If the {@code ReadSupport} has been paused, reading will recommence on it.
     */
    @Override
    public DefaultFileReader resume() {
        paused = false;
        if (resumeHandler != null) {
            Handler<Void> handler = resumeHandler;
            resumeHandler = null;
            handler.handle(null);
        }
        return this;
    }

    @Override
    public DefaultFileReader endHandler(Handler<Void> handler) {
        endHandler = handler;
        return this;
    }

    @Override
    public DefaultFileReader resultHandler(Handler<Result> handler) {
        resultHandler = handler;
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
        handleData(new Buffer(data));
    }

    public void handleData(Buffer data) {
        if (dataHandler != null) {
            dataHandler.handle(data);
        }
    }

    public void handleEnd(Result result) {
        if (resultHandler != null) {
            resultHandler.handle(result);
        }
        if (endHandler != null) {
            endHandler.handle(null);
        }
    }

    public void handleException(Throwable t) {
        if (exceptionHandler != null) {
            exceptionHandler.handle(t);
        }
    }

    public boolean isPaused() {
        return paused;
    }

    public void resumeHandler(Handler<Void> handler) {
        resumeHandler = handler;
    }
}
