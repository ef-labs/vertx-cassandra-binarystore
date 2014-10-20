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

package com.englishtown.vertx;

import com.codahale.metrics.Counter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.englishtown.vertx.cassandra.binarystore.BinaryStoreManager;
import com.englishtown.vertx.cassandra.binarystore.BinaryStoreStarter;
import com.englishtown.vertx.cassandra.binarystore.ChunkInfo;
import com.englishtown.vertx.cassandra.binarystore.Metrics;
import com.englishtown.vertx.cassandra.binarystore.impl.DefaultChunkInfo;
import com.englishtown.vertx.cassandra.binarystore.impl.DefaultFileInfo;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Verticle;

import javax.inject.Inject;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * An EventBus module to save binary files in Cassandra
 */
public class CassandraBinaryStore extends Verticle implements Handler<Message<JsonObject>> {

    public static final String DEFAULT_ADDRESS = "et.cassandra.binarystore";

    private final BinaryStoreStarter starter;
    private final BinaryStoreManager binaryStoreManager;

    protected EventBus eb;
    protected Logger logger;

    private String address;

    private final MetricRegistry registry;
    private JmxReporter reporter;
    private Counter messageCounter;
    private Counter errorCounter;

    @Inject
    public CassandraBinaryStore(BinaryStoreStarter starter, BinaryStoreManager binaryStoreManager, MetricRegistry registry) {
        this.starter = starter;
        this.binaryStoreManager = binaryStoreManager;
        this.registry = registry;
    }

    @Override
    public void start(final Future<Void> startedResult) {

        logger = container.logger();
        messageCounter = registry.counter(name(Metrics.BASE_NAME, "eb.messages"));
        errorCounter = registry.counter(name(Metrics.BASE_NAME, "eb.errors"));

        starter.run()
                .then(aVoid -> {
                    init();
                    startedResult.setResult(aVoid);
                    return null;
                })
                .otherwise(t -> {
                    startedResult.setFailure(t);
                    return null;
                });

    }

    private void init() {

        eb = vertx.eventBus();

        JsonObject config = container.config();
        address = config.getString("address", DEFAULT_ADDRESS);

        // Main Message<JsonObject> handler that inspects an "action" field
        eb.registerHandler(address, this);

        // Message<Buffer> handler to save file chunks
        eb.registerHandler(address + "/saveChunk", message -> {
            messageCounter.inc();
            saveChunk(message);
        });

        // Start jmx metric reporter if enabled
        Map<String, Object> values = new HashMap<>();
        values.put("address", address);
        reporter = com.englishtown.vertx.metrics.Utils.create(this, registry, values, config);

    }

    @Override
    public void stop() {
        if (reporter != null) {
            reporter.stop();
        }
    }

    @Override
    public void handle(Message<JsonObject> message) {

        messageCounter.inc();
        JsonObject jsonObject = message.body();
        String action = getRequiredString("action", message, jsonObject);
        if (action == null) {
            return;
        }

        try {
            switch (action) {
                case "getFile":
                    getFile(message, jsonObject);
                    break;
                case "getChunk":
                    getChunk(message, jsonObject);
                    break;
                case "saveFile":
                    saveFile(message, jsonObject);
                    break;
                default:
                    sendError(message, "action " + action + " is not supported");
            }

        } catch (Throwable e) {
            sendError(message, "Unexpected error in " + action + ": " + e.getMessage(), e);
        }
    }

    public void saveFile(final Message<JsonObject> message, JsonObject jsonObject) {

        UUID id = getUUID("id", message, jsonObject);
        if (id == null) {
            return;
        }

        Long length = getRequiredLong("length", message, jsonObject, 1);
        if (length == null) {
            return;
        }

        Integer chunkSize = getRequiredInt("chunkSize", message, jsonObject, 1);
        if (chunkSize == null) {
            return;
        }

        long uploadDate = jsonObject.getLong("uploadDate", 0);
        if (uploadDate <= 0) {
            uploadDate = System.currentTimeMillis();
        }

        String filename = jsonObject.getString("filename");
        String contentType = jsonObject.getString("contentType");
        JsonObject metadata = jsonObject.getObject("metadata");

        DefaultFileInfo fileInfo = new DefaultFileInfo()
                .setId(id)
                .setLength(length)
                .setChunkSize(chunkSize)
                .setUploadDate(uploadDate)
                .setFileName(filename)
                .setContentType(contentType);

        if (metadata != null) {
            Map<String, String> map = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : metadata.toMap().entrySet()) {
                map.put(entry.getKey(), (entry.getValue() == null ? null : String.valueOf(entry.getValue())));
            }
            fileInfo.setMetadata(map);
        }

        binaryStoreManager.storeFile(fileInfo)
                .then(aVoid -> {
                    sendOK(message);
                    return null;
                })
                .otherwise(t -> {
                    sendError(message, "Error saving file", t);
                    return null;
                });

    }

    /**
     * Handler for saving file chunks.
     *
     * @param message The message body is a Buffer where the first four bytes are an int indicating how many bytes are
     *                the json fields, the remaining bytes are the file chunk to write to Cassandra
     */
    public void saveChunk(Message<Buffer> message) {
        JsonObject jsonObject;
        byte[] data;

        // Parse the byte[] message body
        try {
            Buffer body = message.body();
            if (body.length() == 0) {
                sendError(message, "message body is empty");
                return;
            }

            // First four bytes indicate the json string length
            int len = body.getInt(0);

            // Decode json
            int from = 4;
            byte[] jsonBytes = body.getBytes(from, from + len);
            jsonObject = new JsonObject(decode(jsonBytes));

            // Remaining bytes are the chunk to be written
            from += len;
            data = body.getBytes(from, body.length());

        } catch (RuntimeException e) {
            sendError(message, "error parsing buffer message.  see the documentation for the correct format", e);
            return;
        }

        // Now save the chunk
        saveChunk(message, jsonObject, data);

    }

    public void saveChunk(final Message<Buffer> message, JsonObject jsonObject, byte[] data) {

        if (data == null || data.length == 0) {
            sendError(message, "chunk data is missing");
            return;
        }

        UUID id = getUUID("files_id", message, jsonObject);
        if (id == null) {
            return;
        }

        Integer n = getRequiredInt("n", message, jsonObject, 0);
        if (n == null) {
            return;
        }

        ChunkInfo chunkInfo = new DefaultChunkInfo()
                .setId(id)
                .setNum(n)
                .setData(data);

        binaryStoreManager.storeChunk(chunkInfo)
                .then(aVoid -> {
                    sendOK(message);
                    return null;
                })
                .otherwise(t -> {
                    sendError(message, "Error saving chunk", t);
                    return null;
                });

    }

    public void getFile(final Message<JsonObject> message, JsonObject jsonObject) {

        final UUID id = getUUID("id", message, jsonObject);
        if (id == null) {
            return;
        }

        binaryStoreManager.loadFile(id)
                .then(result -> {
                    if (result == null) {
                        sendError(message, "File " + id.toString() + " does not exist", 404);
                        return null;
                    }

                    JsonObject fileInfo = new JsonObject()
                            .putString("filename", result.getFileName())
                            .putString("contentType", result.getContentType())
                            .putNumber("length", result.getLength())
                            .putNumber("chunkSize", result.getChunkSize())
                            .putNumber("uploadDate", result.getUploadDate());

                    if (result.getMetadata() != null) {
                        JsonObject metadata = new JsonObject();
                        for (Map.Entry<String, String> entry : result.getMetadata().entrySet()) {
                            metadata.putString(entry.getKey(), entry.getValue());
                        }
                        fileInfo.putObject("metadata", metadata);
                    }

                    // Send file info
                    sendOK(message, fileInfo);
                    return null;
                })
                .otherwise(t -> {
                    sendError(message, "Error reading file", t);
                    return null;
                });

    }

    public void getChunk(final Message<JsonObject> message, final JsonObject jsonObject) {

        UUID id = getUUID("files_id", message, jsonObject);

        Integer n = getRequiredInt("n", message, jsonObject, 0);
        if (n == null) {
            return;
        }

        binaryStoreManager.loadChunk(id, n)
                .then(result -> {
                    if (result == null) {
                        message.reply(new byte[0]);
                        return null;
                    }

                    boolean reply = jsonObject.getBoolean("reply", false);
                    Handler<Message<JsonObject>> replyHandler = null;

                    if (reply) {
                        replyHandler = reply1 -> {
                            int n1 = jsonObject.getInteger("n") + 1;
                            jsonObject.putNumber("n", n1);
                            getChunk(reply1, jsonObject);
                        };
                    }

                    // TODO: Change to reply with a Buffer instead of a byte[]?
                    message.reply(result.getData(), replyHandler);
                    return null;
                })
                .otherwise(t -> {
                    sendError(message, "Error reading chunk", t);
                    return null;
                });

    }

    public <T> void sendError(Message<T> message, String error) {
        sendError(message, error, null, null);
    }

    public <T> void sendError(Message<T> message, String error, Throwable e) {
        sendError(message, error, null, e);
    }

    public <T> void sendError(Message<T> message, String error, Integer reason) {
        sendError(message, error, reason, null);
    }

    public <T> void sendError(Message<T> message, String error, Integer reason, Throwable e) {
        errorCounter.inc();
        logger.error(error, e);
        JsonObject result = new JsonObject().putString("status", "error").putString("message", error);
        if (reason != null) {
            result.putNumber("reason", reason);
        }

        message.reply(result);
    }

    public <T> void sendOK(Message<T> message) {
        sendOK(message, new JsonObject());
    }

    public <T> void sendOK(Message<T> message, JsonObject response) {
        response.putString("status", "ok");
        message.reply(response);
    }

    private String decode(byte[] bytes) {
        try {
            return new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // Should never happen
            throw new RuntimeException(e);
        }
    }

    private <T> UUID getUUID(String fieldName, Message<T> message, JsonObject jsonObject) {
        String id = getRequiredString(fieldName, message, jsonObject);
        if (id == null) {
            return null;
        }
        try {
            return UUID.fromString(id);
        } catch (IllegalArgumentException e) {
            sendError(message, fieldName + " " + id + " is not a valid UUID", e);
            return null;
        }
    }

    private <T> String getRequiredString(String fieldName, Message<T> message, JsonObject jsonObject) {
        String value = jsonObject.getString(fieldName);
        if (value == null) {
            sendError(message, fieldName + " must be specified");
        }
        return value;
    }

    private <T> Integer getRequiredInt(String fieldName, Message<T> message, JsonObject jsonObject, int minValue) {
        Integer value = jsonObject.getInteger(fieldName);
        if (value == null) {
            sendError(message, fieldName + " must be specified");
            return null;
        }
        if (value < minValue) {
            sendError(message, fieldName + " must be greater than or equal to " + minValue);
            return null;
        }
        return value;
    }

    private <T> Long getRequiredLong(String fieldName, Message<T> message, JsonObject jsonObject, long minValue) {
        Long value = jsonObject.getLong(fieldName);
        if (value == null) {
            sendError(message, fieldName + " must be specified");
            return null;
        }
        if (value < minValue) {
            sendError(message, fieldName + " must be greater than or equal to " + minValue);
            return null;
        }
        return value;
    }
}
