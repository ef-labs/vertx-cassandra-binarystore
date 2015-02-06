///*
// * The MIT License (MIT)
// * Copyright © 2013 Englishtown <opensource@englishtown.com>
// *
// * Permission is hereby granted, free of charge, to any person obtaining a copy
// * of this software and associated documentation files (the “Software”), to deal
// * in the Software without restriction, including without limitation the rights
// * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// * copies of the Software, and to permit persons to whom the Software is
// * furnished to do so, subject to the following conditions:
// *
// * The above copyright notice and this permission notice shall be included in
// * all copies or substantial portions of the Software.
// *
// * THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// * THE SOFTWARE.
// */
//
//package com.englishtown.integration.java;
//
//import com.englishtown.vertx.cassandra.binarystore.impl.CassandraBinaryStore;
//import io.vertx.core.AsyncResult;
//import io.vertx.core.buffer.Buffer;
//import io.vertx.core.eventbus.EventBus;
//import io.vertx.core.eventbus.Message;
//import io.vertx.core.json.JsonObject;
//import io.vertx.test.core.VertxTestBase;
//import org.junit.Test;
//
//import java.io.UnsupportedEncodingException;
//import java.util.UUID;
//
///**
// * Integration tests for the saveChunk operation
// */
//public class SaveChunkIntegrationTest extends VertxTestBase {
//
//    private EventBus eventBus;
//    private final String address = CassandraBinaryStore.DEFAULT_ADDRESS + "/saveChunk";
//
//    @Override
//    public void setUp() throws Exception {
//        super.setUp();
//
//        eventBus = vertx.eventBus();
//        IntegrationTestHelper.onVerticleStart(this, startedResult);
//    }
//
//    @Test
//    public void testSaveFile_Empty_Bytes() {
//
//        Buffer message = Buffer.buffer();
//        eventBus.send(address, message, (AsyncResult<Message<JsonObject>> reply) -> IntegrationTestHelper.verifyErrorReply(reply, "message body is empty"));
//
//    }
//
//    @Test
//    public void testSaveFile_Invalid_Json() throws Exception {
//
//        byte[] invalid = "{\"property\": 1".getBytes("UTF-8");
//        Buffer buffer = Buffer.buffer();
//        buffer.appendInt(invalid.length);
//        buffer.appendBytes(invalid);
//        buffer.appendBytes(new byte[10]);
//
//        eventBus.send(address, buffer, (AsyncResult<Message<JsonObject>> reply) -> IntegrationTestHelper.verifyErrorReply(reply, "error parsing buffer message.  see the documentation for the correct format"));
//
//    }
//
//    @Test
//    public void testSaveFile_No_Data() throws Exception {
//
//        Buffer message = getMessage(new JsonObject(), new byte[0]);
//
//        eventBus.send(address, message, (AsyncResult<Message<JsonObject>> reply) -> IntegrationTestHelper.verifyErrorReply(reply, "chunk data is missing"));
//
//    }
//
//    @Test
//    public void testSaveFile_Empty_Json() throws Exception {
//
//        Buffer message = getMessage(new JsonObject(), new byte[10]);
//
//        eventBus.send(address, message, (AsyncResult<Message<JsonObject>> reply) -> IntegrationTestHelper.verifyErrorReply(reply, "files_id must be specified"));
//
//    }
//
//    @Test
//    public void testSaveFile_Missing_N() throws Exception {
//
//        UUID id = UUID.randomUUID();
//        JsonObject jsonObject = new JsonObject().put("files_id", id.toString());
//        Buffer message = getMessage(jsonObject, new byte[10]);
//
//        eventBus.send(address, message, (AsyncResult<Message<JsonObject>> reply) -> IntegrationTestHelper.verifyErrorReply(reply, "n must be specified"));
//
//    }
//
//    @Test
//    public void testSaveFile() throws Exception {
//
//        String files_id = UUID.randomUUID().toString();
//        int n = 0;
//
//        final JsonObject jsonObject = new JsonObject()
//                .put("files_id", files_id)
//                .put("n", n);
//
//        Buffer message = getMessage(jsonObject, new byte[10]);
//
//        eventBus.send(address, message, (AsyncResult<Message<JsonObject>> reply1) -> {
//            VertxAssert.assertEquals("ok", reply1.result().body().getString("status"));
//
//            jsonObject.put("action", "getChunk");
//
//            eventBus.send(CassandraBinaryStore.DEFAULT_ADDRESS, jsonObject, (AsyncResult<Message<byte[]>> reply2) -> {
//                VertxAssert.assertEquals(10, reply2.result().body().length);
//                testComplete();
//            });
//        });
//
//    }
//
//    private Buffer getMessage(JsonObject jsonObject, byte[] data) throws UnsupportedEncodingException {
//
//        Buffer buffer = Buffer.buffer();
//        byte[] jsonBytes = jsonObject.encode().getBytes("UTF-8");
//
//        buffer.appendInt(jsonBytes.length);
//        buffer.appendBytes(jsonBytes);
//        buffer.appendBytes(data);
//
//        return buffer;
//    }
//
//}
