/*
 * The MIT License (MIT)
 * Copyright © 2013 Englishtown <opensource@englishtown.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the “Software”), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package com.englishtown.integration.java;

import com.englishtown.vertx.CassandraBinaryStore;
import org.junit.Test;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;

import java.util.UUID;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.testComplete;

/**
 * Integration tests for the saveFile operation
 */
public class SaveFileIntegrationTest extends TestVerticle {

    private EventBus eventBus;
    private JsonObject config;
    private final String address = CassandraBinaryStore.DEFAULT_ADDRESS;

    @Test
    public void testSaveFile_Empty_Json() {

        eventBus.send(address, new JsonObject(), (Message<JsonObject> reply) -> IntegrationTestHelper.verifyErrorReply(reply, "action must be specified"));

    }

    @Test
    public void testSaveFile_Missing_ID() {

        JsonObject message = new JsonObject()
                .putString("action", "saveFile");

        eventBus.send(address, message, (Message<JsonObject> reply) -> IntegrationTestHelper.verifyErrorReply(reply, "id must be specified"));

    }

    @Test
    public void testSaveFile_Missing_Length() {

        JsonObject message = new JsonObject()
                .putString("action", "saveFile")
                .putString("id", UUID.randomUUID().toString());

        eventBus.send(address, message, (Message<JsonObject> reply) -> IntegrationTestHelper.verifyErrorReply(reply, "length must be specified"));

    }

    @Test
    public void testSaveFile_Missing_ChunkSize() {

        JsonObject message = new JsonObject()
                .putString("action", "saveFile")
                .putString("id", UUID.randomUUID().toString())
                .putNumber("length", 1024 * 1024);

        eventBus.send(address, message, (Message<JsonObject> reply) -> IntegrationTestHelper.verifyErrorReply(reply, "chunkSize must be specified"));

    }

    @Test
    public void testSaveFile_All() {

        final UUID id = UUID.randomUUID();
        final int length = 1024 * 1024;
        final int chunkSize = 102400;
        final long uploadDate = System.currentTimeMillis();
        final String filename = "integration_test.jpg";
        final String contentType = "image/jpeg";

        JsonObject message = new JsonObject()
                .putString("action", "saveFile")
                .putString("id", id.toString())
                .putNumber("length", length)
                .putNumber("chunkSize", chunkSize)
                .putNumber("uploadDate", uploadDate)
                .putString("filename", filename)
                .putString("contentType", contentType)
                .putObject("metadata", new JsonObject().putString("additional", "info"));

        eventBus.send(address, message, (Message<JsonObject> reply) -> {
            VertxAssert.assertEquals("ok", reply.body().getString("status"));
            testComplete();
        });

    }

    @Override
    public void start(Future<Void> startedResult) {
        eventBus = vertx.eventBus();
        config = IntegrationTestHelper.onVerticleStart(this, startedResult);
    }

}
