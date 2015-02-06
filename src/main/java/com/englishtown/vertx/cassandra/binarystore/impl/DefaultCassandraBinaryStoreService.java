///*
//* The MIT License (MIT)
//* Copyright © 2013 Englishtown <opensource@englishtown.com>
//*
//* Permission is hereby granted, free of charge, to any person obtaining a copy
//* of this software and associated documentation files (the “Software”), to deal
//* in the Software without restriction, including without limitation the rights
//* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//* copies of the Software, and to permit persons to whom the Software is
//* furnished to do so, subject to the following conditions:
//*
//* The above copyright notice and this permission notice shall be included in
//* all copies or substantial portions of the Software.
//*
//* THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
//* THE SOFTWARE.
//*/
//
//package com.englishtown.vertx.cassandra.binarystore.impl;
//
//import com.codahale.metrics.Counter;
//import com.codahale.metrics.MetricRegistry;
//import com.englishtown.vertx.cassandra.binarystore.*;
//import io.vertx.core.AsyncResult;
//import io.vertx.core.Future;
//import io.vertx.core.Handler;
//import io.vertx.core.buffer.Buffer;
//import io.vertx.core.eventbus.Message;
//import io.vertx.core.json.JsonObject;
//import io.vertx.core.logging.Logger;
//import io.vertx.core.logging.impl.LoggerFactory;
//
//import javax.inject.Inject;
//import java.io.UnsupportedEncodingException;
//import java.util.Map;
//import java.util.UUID;
//
//import static com.codahale.metrics.MetricRegistry.name;
//
///**
// * An EventBus module to save binary files in Cassandra
// */
//public class DefaultCassandraBinaryStoreService implements CassandraBinaryStoreService {
//
//    public static final String DEFAULT_ADDRESS = "et.cassandra.binarystore";
//    private static final Logger logger = LoggerFactory.getLogger(DefaultCassandraBinaryStoreService.class);
//
//    private final BinaryStoreStarter starter;
//    private final BinaryStoreManager binaryStoreManager;
//
//    private final MetricRegistry registry;
//    //    private JmxReporter reporter;
//    private Counter messageCounter;
//    private Counter errorCounter;
//
//    @Inject
//    public DefaultCassandraBinaryStoreService(BinaryStoreStarter starter, BinaryStoreManager binaryStoreManager, MetricRegistry registry) {
//        this.starter = starter;
//        this.binaryStoreManager = binaryStoreManager;
//        this.registry = registry;
//    }
//
//    @Override
//    public void start(final Future<Void> startFuture) {
//
//        messageCounter = registry.counter(name(Metrics.BASE_NAME, "eb.messages"));
//        errorCounter = registry.counter(name(Metrics.BASE_NAME, "eb.errors"));
//
//        starter.run()
//                .then(aVoid -> {
//                    startFuture.complete();
//                    return null;
//                })
//                .otherwise(t -> {
//                    startFuture.fail(t);
//                    return null;
//                });
//
//    }
//
//    @Override
//    public void stop() {
////        if (reporter != null) {
////            reporter.stop();
////        }
//    }
//
////    @Override
////    public void handle(Message<JsonObject> message) {
////
////        messageCounter.inc();
////        JsonObject jsonObject = message.body();
////        String action = getRequiredString("action", message, jsonObject);
////        if (action == null) {
////            return;
////        }
////
////        try {
////            switch (action) {
////                case "getFile":
////                    getFile(message, jsonObject);
////                    break;
////                case "getChunk":
////                    getChunk(message, jsonObject);
////                    break;
////                case "saveFile":
////                    saveFile(message, jsonObject);
////                    break;
////                default:
////                    sendError(message, "action " + action + " is not supported");
////            }
////
////        } catch (Throwable e) {
////            sendError(message, "Unexpected error in " + action + ": " + e.getMessage(), e);
////        }
////    }
//
//
//    @Override
//    public void getFile(String id, Handler<AsyncResult<JsonObject>> resultHandler) {
//
//        UUID file_id = UUID.fromString(id);
//
//        binaryStoreManager.loadFile(file_id)
//                .then(result -> {
//                    if (result == null) {
//                        resultHandler.handle(Future.succeededFuture());
//                    } else {
//                        resultHandler.handle(Future.succeededFuture(result.toJson()));
//                    }
//                    return null;
//                })
//                .otherwise(t -> {
//                    resultHandler.handle(Future.failedFuture(t));
//                    return null;
//                });
//
//    }
//
//    @Override
//    public void saveFile(FileInfo fileInfo, Handler<AsyncResult<Void>> resultHandler) {
//
//        binaryStoreManager.storeFile(fileInfo)
//                .then(aVoid -> {
//                    resultHandler.handle(Future.succeededFuture());
//                    return null;
//                })
//                .otherwise(t -> {
//                    resultHandler.handle(Future.failedFuture(t));
//                    return null;
//                });
//
//    }
//
//    @Override
//    public void getChunk(String files_id, int n, Handler<AsyncResult<Buffer>> resultHandler) {
//
//        UUID id = UUID.fromString(files_id);
//
//        binaryStoreManager.loadChunk(id, n)
//                .then(result -> {
//                    if (result == null) {
//                        resultHandler.handle(Future.succeededFuture());
//                    } else {
//                        resultHandler.handle(Future.succeededFuture(Buffer.buffer(result.getData())));
//                    }
//                    return null;
//                })
//                .otherwise(t -> {
//                    resultHandler.handle(Future.failedFuture(t));
//                    return null;
//                });
//
//    }
//
//    @Override
//    public void saveChunk(String id, int n, byte[] data, Handler<AsyncResult<Void>> resultHandler) {
//
//        if (id == null) {
//            throw new IllegalArgumentException("id is null");
//        }
//        UUID files_id = UUID.fromString(id);
//
//        if (data == null || data.length == 0) {
//            throw new IllegalArgumentException("chunk data is missing");
//        }
//
//        if (n < 0) {
//            throw new IllegalArgumentException("n must be >= 0");
//        }
//
//        ChunkInfo chunkInfo = new DefaultChunkInfo()
//                .setId(files_id)
//                .setNum(n)
//                .setData(data);
//
//        binaryStoreManager.storeChunk(chunkInfo)
//                .then(aVoid -> {
//                    resultHandler.handle(Future.succeededFuture());
//                    return null;
//                })
//                .otherwise(t -> {
//                    resultHandler.handle(Future.failedFuture(t));
//                    return null;
//                });
//
//    }
//
//    /**
//     * Handler for saving file chunks.
//     *
//     * @param jsonAndData The first four bytes are an int indicating how many bytes are
//     *                    the json fields, the remaining bytes are the file chunk to write to Cassandra
//     */
//    @Override
//    public void saveChunk(Buffer jsonAndData, Handler<AsyncResult<Void>> resultHandler) {
//
//        // First four bytes indicate the json string length
//        int len = jsonAndData.getInt(0);
//
//        // Decode json
//        int from = 4;
//        byte[] jsonBytes = jsonAndData.getBytes(from, from + len);
//        JsonObject json = new JsonObject(decode(jsonBytes));
//
//        // Remaining bytes are the chunk to be written
//        from += len;
//        byte[] data = jsonAndData.getBytes(from, jsonAndData.length());
//
//        saveChunk(json.getString("files_id"), json.getInteger("n"), data, resultHandler);
//
//    }
//
//    private String decode(byte[] bytes) {
//        try {
//            return new String(bytes, "UTF-8");
//        } catch (UnsupportedEncodingException e) {
//            // Should never happen
//            throw new RuntimeException(e);
//        }
//    }
//
//}
