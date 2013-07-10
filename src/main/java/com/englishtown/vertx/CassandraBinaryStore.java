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

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Verticle;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.UUID;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

/**
 * An EventBus module to save binary files in Cassandra
 */
public class CassandraBinaryStore extends Verticle implements Handler<Message<JsonObject>> {

    public static final String DEFAULT_ADDRESS = "et.cassandra.binarystore";

    protected EventBus eb;
    protected Logger logger;

    protected String address;
    protected String ip;
    protected int port;
    protected String keyspace;

    protected Cluster cluster;
    protected Session session;
    protected PreparedStatement insertChunk;

    public static final String DEFAULT_TABLE_NAME = "files";

    @Override
    public void start() {
        eb = vertx.eventBus();
        logger = container.logger();

        JsonObject config = container.config();
        address = config.getString("address", DEFAULT_ADDRESS);

        ip = config.getString("ip", "127.0.0.1");
        keyspace = config.getString("keyspace", "binarystore");

        cluster = Cluster.builder().addContactPoint(ip).build();
        session = cluster.connect();

        ensureSchema();
        initPreparedStatements();

        // Main Message<JsonObject> handler that inspects an "action" field
        eb.registerHandler(address, this);

        // Message<Buffer> handler to save file chunks
        eb.registerHandler(address + "/saveChunk", new Handler<Message<Buffer>>() {
            @Override
            public void handle(Message<Buffer> message) {
                saveChunk(message);
            }
        });

    }

    @Override
    public void stop() {
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    public void ensureSchema() {

        // Ensure the keyspace exists
        try {
            session.execute("CREATE KEYSPACE " + keyspace + " WITH replication " +
                    "= {'class':'SimpleStrategy', 'replication_factor':3};");
        } catch (AlreadyExistsException e) {
            // OK if it already exists
        }

        try {
            session.execute(
                    "CREATE TABLE " + keyspace + ".files (" +
                            "id uuid PRIMARY KEY," +
                            "filename text," +
                            "contentType text," +
                            "chunkSize int," +
                            "length bigint," +
                            "uploadDate bigint," +
                            "metadata text" +
                            ");");

        } catch (AlreadyExistsException e) {
            // OK if it already exists
        }

        try {
            session.execute(
                    "CREATE TABLE " + keyspace + ".chunks (" +
                            "files_id uuid," +
                            "n int," +
                            "data blob," +
                            "PRIMARY KEY (files_id, n)" +
                            ");");

        } catch (AlreadyExistsException e) {
            // OK if it already exists
        }

    }

    public void initPreparedStatements() {

        StringBuffer sb = new StringBuffer()
                .append("INSERT INTO ")
                .append(keyspace)
                .append(".chunks (files_id, n, data) VALUES(?, ?, ?)");

        insertChunk = session.prepare(sb.toString());

    }

    @Override
    public void handle(Message<JsonObject> message) {

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

    public void saveFile(Message<JsonObject> message, JsonObject jsonObject) {

        UUID id = getUUID("id", message, jsonObject);
        if (id == null) {
            return;
        }

        Integer length = getRequiredInt("length", message, jsonObject, 1);
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

        try {
            Insert insert = QueryBuilder
                    .insertInto(keyspace, "files")
                    .value("id", id)
                    .value("length", length)
                    .value("chunkSize", chunkSize)
                    .value("uploadDate", uploadDate);

            if (filename != null) insert.value("filename", filename);
            if (contentType != null) insert.value("contentType", contentType);
            if (metadata != null) insert.value("metadata", metadata.encode());  // TODO Store metadata as a map?

            session.execute(insert);
            sendOK(message);

        } catch (Exception e) {
            sendError(message, "Error saving file", e);
        }
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
            sendError(message, "error parsing byte[] message.  see the documentation for the correct format", e);
            return;
        }

        // Now save the chunk
        saveChunk(message, jsonObject, data);

    }

    public void saveChunk(Message<Buffer> message, JsonObject jsonObject, byte[] data) {

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

        try {
            BoundStatement insert = insertChunk.bind(id, n, ByteBuffer.wrap(data));
            session.execute(insert);
            sendOK(message);

        } catch (RuntimeException e) {
            sendError(message, "Error saving chunk", e);
        }

    }

    public void getFile(Message<JsonObject> message, JsonObject jsonObject) {

        UUID id = getUUID("id", message, jsonObject);
        if (id == null) {
            return;
        }

        try {
            Query query = QueryBuilder
                    .select()
                    .all()
                    .from(keyspace, "files")
                    .where(eq("id", id));

            Row row = session.execute(query).one();

            if (row == null) {
                sendError(message, "File " + id.toString() + " does not exist");
                return;
            }

            JsonObject fileInfo = new JsonObject()
                    .putString("filename", row.getString("filename"))
                    .putString("contentType", row.getString("contentType"))
                    .putNumber("length", row.getLong("length"))
                    .putNumber("chunkSize", row.getInt("chunkSize"))
                    .putNumber("uploadDate", row.getLong("uploadDate"));

            String metadata = row.getString("metadata");
            if (metadata != null) {
                fileInfo.putObject("metadata", new JsonObject(metadata));
            }

            // Send file info
            sendOK(message, fileInfo);


        } catch (RuntimeException e) {
            sendError(message, "Error reading file", e);
        }

    }

    public void getChunk(Message<JsonObject> message, final JsonObject jsonObject) {

        UUID id = getUUID("files_id", message, jsonObject);

        Integer n = getRequiredInt("n", message, jsonObject, 0);
        if (n == null) {
            return;
        }

        Query select = QueryBuilder
                .select("data")
                .from(keyspace, "chunks")
                .where(eq("files_id", id)).and(eq("n", n));

        Row row = session.execute(select).one();

        if (row == null) {
            message.reply(new byte[0]);
            return;
        }

        ByteBuffer bb = row.getBytes("data");
        byte[] data = new byte[bb.remaining()];
        bb.get(data);

        boolean reply = jsonObject.getBoolean("reply", false);
        Handler<Message<JsonObject>> replyHandler = null;

        if (reply) {
            replyHandler = new Handler<Message<JsonObject>>() {
                @Override
                public void handle(Message<JsonObject> reply) {
                    int n = jsonObject.getInteger("n") + 1;
                    jsonObject.putNumber("n", n);
                    getChunk(reply, jsonObject);
                }
            };
        }

        // TODO: Change to reply with a Buffer instead of a byte[]?
        message.reply(data, replyHandler);

    }

    public <T> void sendError(Message<T> message, String error) {
        sendError(message, error, null);
    }

    public <T> void sendError(Message<T> message, String error, Throwable e) {
        logger.error(error, e);
        JsonObject result = new JsonObject().putString("status", "error").putString("message", error);
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

}
