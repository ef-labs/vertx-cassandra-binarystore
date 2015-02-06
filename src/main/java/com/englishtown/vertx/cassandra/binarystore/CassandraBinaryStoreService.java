//package com.englishtown.vertx.cassandra.binarystore;
//
//import io.vertx.codegen.annotations.GenIgnore;
//import io.vertx.codegen.annotations.ProxyGen;
//import io.vertx.codegen.annotations.ProxyIgnore;
//import io.vertx.codegen.annotations.VertxGen;
//import io.vertx.core.AsyncResult;
//import io.vertx.core.Future;
//import io.vertx.core.Handler;
//import io.vertx.core.buffer.Buffer;
//import io.vertx.core.json.JsonObject;
//
//import java.io.UnsupportedEncodingException;
//
///**
// * Cassandra Binary Store service
// */
//@VertxGen
//@ProxyGen
//public interface CassandraBinaryStoreService {
//
//    @GenIgnore
//    @ProxyIgnore
//    void start(Future<Void> startFuture);
//
//    @GenIgnore
//    @ProxyIgnore
//    void stop();
//
//    void getFile(String id, Handler<AsyncResult<JsonObject>> resultHandler);
//
//    void saveFile(FileInfo fileInfo, Handler<AsyncResult<Void>> resultHandler);
//
//    void getChunk(String files_id, int n, Handler<AsyncResult<Buffer>> resultHandler);
//
//    @GenIgnore
//    @ProxyIgnore
//    default void saveChunk(String id, int n, byte[] data, Handler<AsyncResult<Void>> resultHandler) {
//        try {
//            JsonObject jsonObject = new JsonObject()
//                    .put("files_id", id)
//                    .put("n", n);
//
//            int len = data.length;
//            byte[] jsonBytes = jsonObject.encode().getBytes("UTF-8");
//            Buffer buffer = Buffer.buffer(len + 4 + jsonBytes.length);
//
//            buffer.appendInt(jsonBytes.length);
//            buffer.appendBytes(jsonBytes);
//            buffer.appendBytes(data);
//
//            saveChunk(buffer, resultHandler);
//
//        } catch (UnsupportedEncodingException e) {
//            throw new RuntimeException(e);
//        }
//
//    }
//
//    void saveChunk(Buffer jsonAndData, Handler<AsyncResult<Void>> resultHandler);
//
//}
