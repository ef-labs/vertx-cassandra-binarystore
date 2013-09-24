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
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.englishtown.jmx.BeanManager;
import com.englishtown.vertx.mxbeans.impl.CassandraGeneralInfoMXBeanImpl;
import com.englishtown.vertx.mxbeans.impl.ChunksClientStatisticsMXBean;
import com.englishtown.vertx.mxbeans.impl.FilesClientStatisticsMXBean;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Verticle;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

/**
 * An EventBus module to save binary files in Cassandra
 */
public class CassandraBinaryStore extends Verticle implements Handler<Message<JsonObject>> {

    public static final String DEFAULT_ADDRESS = "et.cassandra.binarystore";
    private final Provider<Cluster.Builder> clusterBuilderProvider;

    protected EventBus eb;
    protected Logger logger;

    protected String keyspace;

    protected Cluster cluster;
    protected Session session;
    protected PreparedStatement insertChunk;
    protected PreparedStatement insertFile;
    protected PreparedStatement getChunk;
    protected PreparedStatement getFile;
    private String address;
    private JsonObject config;
    private FilesClientStatisticsMXBean filesStatsBean;
    private ChunksClientStatisticsMXBean chunksStatsBean;

    @Inject
    public CassandraBinaryStore(Provider<Cluster.Builder> clusterBuilderProvider) {
        if (clusterBuilderProvider == null) {
            throw new IllegalArgumentException("clusterBuilderProvider is required");
        }
        this.clusterBuilderProvider = clusterBuilderProvider;
        this.filesStatsBean = FilesClientStatisticsMXBean.INSTANCE;
        this.chunksStatsBean = ChunksClientStatisticsMXBean.INSTANCE;
    }

    @Override
    public void start(final Future<Void> startedResult) {
        eb = vertx.eventBus();
        logger = container.logger();

        config = container.config();
        address = config.getString("address", DEFAULT_ADDRESS);

        // Get keyspace, default to binarystore
        keyspace = config.getString("keyspace", "binarystore");

        // Build cluster and session
        Cluster.Builder builder = getBuilder(config);
        cluster = builder.build();
        session = cluster.connect();

        ensureSchema();
        initPreparedStatements(config);

        // Main Message<JsonObject> handler that inspects an "action" field
        eb.registerHandler(address, this);

        // Message<Buffer> handler to save file chunks
        eb.registerHandler(address + "/saveChunk", new Handler<Message<Buffer>>() {
            @Override
            public void handle(Message<Buffer> message) {
                saveChunk(message);
            }
        });

        if (BeanManager.INSTANCE.isEnabled()) {
            try {
                registerBeans();
            } catch (Exception e) {
                startedResult.setFailure(e);
                startedResult.failed();
                return;
            }
        }

        startedResult.setResult(null);
    }

    public Cluster.Builder getBuilder(JsonObject config) {

        // Create cluster builder
        Cluster.Builder builder = clusterBuilderProvider.get();

        // Get array of IPs, default to localhost
        JsonArray ips = config.getArray("ips");
        if (ips == null || ips.size() == 0) {
            ips = new JsonArray().addString("127.0.0.1");
        }

        // Add cassandra cluster contact points
        for (int i = 0; i < ips.size(); i++) {
            builder.addContactPoint(ips.<String>get(i));
        }

        initPoolingOptions(builder, config);
        initPolicies(builder, config);

        return builder;
    }

    public void initPoolingOptions(Cluster.Builder builder, JsonObject config) {

        JsonObject poolingConfig = config.getObject("pooling");

        if (poolingConfig == null) {
            return;
        }

        PoolingOptions poolingOptions = builder.poolingOptions();

        Integer core_connections_per_host_local = poolingConfig.getInteger("core_connections_per_host_local");
        Integer core_connections_per_host_remote = poolingConfig.getInteger("core_connections_per_host_remote");
        Integer max_connections_per_host_local = poolingConfig.getInteger("max_connections_per_host_local");
        Integer max_connections_per_host_remote = poolingConfig.getInteger("max_connections_per_host_remote");
        Integer min_simultaneous_requests_local = poolingConfig.getInteger("min_simultaneous_requests_local");
        Integer min_simultaneous_requests_remote = poolingConfig.getInteger("min_simultaneous_requests_remote");
        Integer max_simultaneous_requests_local = poolingConfig.getInteger("max_simultaneous_requests_local");
        Integer max_simultaneous_requests_remote = poolingConfig.getInteger("max_simultaneous_requests_remote");

        if (core_connections_per_host_local != null) {
            poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, core_connections_per_host_local.intValue());
        }
        if (core_connections_per_host_remote != null) {
            poolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE, core_connections_per_host_remote.intValue());
        }
        if (max_connections_per_host_local != null) {
            poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, max_connections_per_host_local.intValue());
        }
        if (max_connections_per_host_remote != null) {
            poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, max_connections_per_host_remote.intValue());
        }
        if (min_simultaneous_requests_local != null) {
            poolingOptions.setMinSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, min_simultaneous_requests_local.intValue());
        }
        if (min_simultaneous_requests_remote != null) {
            poolingOptions.setMinSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE, min_simultaneous_requests_remote.intValue());
        }
        if (max_simultaneous_requests_local != null) {
            poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, max_simultaneous_requests_local.intValue());
        }
        if (max_simultaneous_requests_remote != null) {
            poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE, max_simultaneous_requests_remote.intValue());
        }

    }

    public void initPolicies(Cluster.Builder builder, JsonObject config) {

        JsonObject policyConfig = config.getObject("policies");

        if (policyConfig == null) {
            return;
        }

        JsonObject loadBalancing = policyConfig.getObject("load_balancing");
        if (loadBalancing != null) {
            String name = loadBalancing.getString("name");

            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("A load balancing policy must have a class name field");

            } else if ("DCAwareRoundRobinPolicy".equalsIgnoreCase(name)
                    || "com.datastax.driver.core.policies.DCAwareRoundRobinPolicy".equalsIgnoreCase(name)) {

                String localDc = loadBalancing.getString("local_dc");
                int usedHostsPerRemoteDc = loadBalancing.getInteger("used_hosts_per_remote_dc", 0);

                if (localDc == null || localDc.isEmpty()) {
                    throw new IllegalArgumentException("A DCAwareRoundRobinPolicy requires a local_dc in configuration.");
                }

                builder.withLoadBalancingPolicy(new DCAwareRoundRobinPolicy(localDc, usedHostsPerRemoteDc));

            } else {

                Class<?> clazz;
                try {
                    clazz = Thread.currentThread().getContextClassLoader().loadClass(name);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
                if (LoadBalancingPolicy.class.isAssignableFrom(clazz)) {
                    try {
                        builder.withLoadBalancingPolicy((LoadBalancingPolicy) clazz.newInstance());
                    } catch (IllegalAccessException | InstantiationException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    throw new IllegalArgumentException("Class " + name + " does not implement LoadBalancingPolicy");
                }

            }
        }

    }

    private void registerBeans() throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        // Register General Info Bean
        final CassandraGeneralInfoMXBeanImpl generalInfoMXBean = new CassandraGeneralInfoMXBeanImpl(address, container.config(),
                vertx.isWorker(), config.getArray("ips"));
        final Hashtable<String, String> generalInfoKeys = new Hashtable<>();
        generalInfoKeys.put("type", "GeneralInfo");
        generalInfoKeys.put("verticle", this.getClass().getSimpleName());
        BeanManager.INSTANCE.registerBean(generalInfoMXBean, generalInfoKeys);
    }

    @Override
    public void stop() {
        if (session != null) {
            session.shutdown();
        }
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    public void ensureSchema() {

        Metadata metadata = cluster.getMetadata();

        // Ensure the keyspace exists
        KeyspaceMetadata kmd = metadata.getKeyspace(keyspace);
        if (kmd == null) {
            try {
                session.execute("CREATE KEYSPACE " + keyspace + " WITH replication " +
                        "= {'class':'SimpleStrategy', 'replication_factor':3};");
            } catch (AlreadyExistsException e) {
                // OK if it already exists
            }
        }

        if (kmd == null || kmd.getTable("files") == null) {
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
        }

        if (kmd == null || kmd.getTable("chunks") == null) {
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

    }

    public ConsistencyLevel getQueryConsistencyLevel(JsonObject config) {
        String consistency = config.getString("consistency_level");

        if (consistency == null || consistency.isEmpty()) {
            return null;
        }

        if (consistency.equalsIgnoreCase("ANY")) {
            return ConsistencyLevel.ANY;
        }
        if (consistency.equalsIgnoreCase("ONE")) {
            return ConsistencyLevel.ONE;
        }
        if (consistency.equalsIgnoreCase("TWO")) {
            return ConsistencyLevel.TWO;
        }
        if (consistency.equalsIgnoreCase("THREE")) {
            return ConsistencyLevel.THREE;
        }
        if (consistency.equalsIgnoreCase("QUORUM")) {
            return ConsistencyLevel.QUORUM;
        }
        if (consistency.equalsIgnoreCase("ALL")) {
            return ConsistencyLevel.ALL;
        }
        if (consistency.equalsIgnoreCase("LOCAL_QUORUM")) {
            return ConsistencyLevel.LOCAL_QUORUM;
        }
        if (consistency.equalsIgnoreCase("EACH_QUORUM")) {
            return ConsistencyLevel.EACH_QUORUM;
        }

        throw new IllegalArgumentException("'" + consistency + "' is not a valid consistency level.");
    }

    public void initPreparedStatements(JsonObject config) {

        String query = QueryBuilder
                .insertInto(keyspace, "chunks")
                .value("files_id", bindMarker())
                .value("n", bindMarker())
                .value("data", bindMarker())
                .getQueryString();

        this.insertChunk = session.prepare(query);

        query = QueryBuilder
                .insertInto(keyspace, "files")
                .value("id", bindMarker())
                .value("length", bindMarker())
                .value("chunkSize", bindMarker())
                .value("uploadDate", bindMarker())
                .value("filename", bindMarker())
                .value("contentType", bindMarker())
                .value("metadata", bindMarker())
                .getQueryString();

        this.insertFile = session.prepare(query);

        query = QueryBuilder
                .select()
                .all()
                .from(keyspace, "files")
                .where(eq("id", bindMarker()))
                .getQueryString();

        this.getFile = session.prepare(query);

        query = QueryBuilder
                .select("data")
                .from(keyspace, "chunks")
                .where(eq("files_id", bindMarker()))
                .and(eq("n", bindMarker()))
                .getQueryString();

        this.getChunk = session.prepare(query);

        // Get query consistency level
        ConsistencyLevel consistency = getQueryConsistencyLevel(config);
        if (consistency != null) {
            insertChunk.setConsistencyLevel(consistency);
            insertFile.setConsistencyLevel(consistency);
            getFile.setConsistencyLevel(consistency);
            getChunk.setConsistencyLevel(consistency);
        }

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

    public void saveFile(final Message<JsonObject> message, JsonObject jsonObject) {
        final Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();

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
        String metadataStr = metadata == null ? null : metadata.encode();
        // TODO Store metadata as a map?

        BoundStatement query = insertFile.bind(id, length, chunkSize, uploadDate, filename, contentType, metadataStr);

        executeQuery(query, message, new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(ResultSet result) {
                stopwatch.stop();
                filesStatsBean.addToWriteStats(stopwatch.elapsed(TimeUnit.MILLISECONDS));
                sendOK(message);
            }

            @Override
            public void onFailure(Throwable t) {
                filesStatsBean.incrementWriteErrorCount();
                sendError(message, "Error saving file", t);
            }
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
                chunksStatsBean.incrementWriteErrorCount();
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
            chunksStatsBean.incrementWriteErrorCount();
            sendError(message, "error parsing buffer message.  see the documentation for the correct format", e);
            return;
        }

        // Now save the chunk
        saveChunk(message, jsonObject, data);

    }

    public void saveChunk(final Message<Buffer> message, JsonObject jsonObject, byte[] data) {
        final Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();

        if (data == null || data.length == 0) {
            chunksStatsBean.incrementWriteErrorCount();
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

        BoundStatement insert = insertChunk.bind(id, n, ByteBuffer.wrap(data));

        executeQuery(insert, message, new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(ResultSet result) {
                stopwatch.stop();
                chunksStatsBean.addToWriteStats(stopwatch.elapsed(TimeUnit.MILLISECONDS));
                sendOK(message);
            }

            @Override
            public void onFailure(Throwable t) {
                chunksStatsBean.incrementWriteErrorCount();
                sendError(message, "Error saving chunk", t);
            }
        });

    }

    public void getFile(final Message<JsonObject> message, JsonObject jsonObject) {
        final Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();

        final UUID id = getUUID("id", message, jsonObject);
        if (id == null) {
            return;
        }

        BoundStatement query = getFile.bind(id);

        executeQuery(query, message, new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(ResultSet result) {
                Row row = result.one();
                if (row == null) {
                    filesStatsBean.incrementReadErrorCount();
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

                stopwatch.stop();
                filesStatsBean.addToReadStats(stopwatch.elapsed(TimeUnit.MILLISECONDS));

                // Send file info
                sendOK(message, fileInfo);
            }

            @Override
            public void onFailure(Throwable t) {
                filesStatsBean.incrementReadErrorCount();
                sendError(message, "Error reading file", t);
            }
        });

    }

    public void getChunk(final Message<JsonObject> message, final JsonObject jsonObject) {
        final Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();

        UUID id = getUUID("files_id", message, jsonObject);

        Integer n = getRequiredInt("n", message, jsonObject, 0);
        if (n == null) {
            return;
        }

        BoundStatement query = getChunk.bind(id, n);

        executeQuery(query, message, new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(ResultSet result) {
                Row row = result.one();
                if (row == null) {
                    chunksStatsBean.incrementReadErrorCount();
                    message.reply(new byte[0]);
                    return;
                }

                ByteBuffer bb = row.getBytes("data");
                byte[] data = new byte[bb.remaining()];
                bb.get(data);

                stopwatch.stop();
                chunksStatsBean.addToReadStats(stopwatch.elapsed(TimeUnit.MILLISECONDS));

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

            @Override
            public void onFailure(Throwable t) {
                chunksStatsBean.incrementReadErrorCount();
                sendError(message, "Error reading chunk", t);
            }
        });

    }

    public <T> void executeQuery(Query query, Message<T> message, final FutureCallback<ResultSet> callback) {

        try {
            final ResultSetFuture future = session.executeAsync(query);
            Futures.addCallback(future, callback);

        } catch (Throwable e) {
            sendError(message, "Error executing async cassandra query", e);
        }

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
