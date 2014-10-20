package com.englishtown.vertx.cassandra.binarystore.impl;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;
import com.englishtown.promises.Promise;
import com.englishtown.promises.When;
import com.englishtown.vertx.cassandra.binarystore.*;
import com.englishtown.vertx.cassandra.promises.WhenCassandraSession;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Default implementation of {@link com.englishtown.vertx.cassandra.binarystore.BinaryStoreManager}
 */
public class DefaultBinaryStoreManager implements BinaryStoreManager {

    private final MetricRegistry registry;
    private final When when;

    private final WhenCassandraSession session;
    private final BinaryStoreStatements statements;
    private final Metrics fileMetrics;
    private final Metrics chunkMetrics;

    @Inject
    public DefaultBinaryStoreManager(WhenCassandraSession session, BinaryStoreStatements statements, MetricRegistry registry, When when) {
        this.session = session;
        this.statements = statements;
        this.registry = registry;
        this.when = when;

        this.fileMetrics = new Metrics(registry, "files");
        this.chunkMetrics = new Metrics(registry, "chunks");
    }

    @Override
    public Promise<Void> storeFile(FileInfo fileInfo) {

        final Metrics.Context context = fileMetrics.timeWrite();

        BoundStatement insert = statements
                .getStoreFile()
                .bind(
                        fileInfo.getId(),
                        fileInfo.getLength(),
                        fileInfo.getChunkSize(),
                        fileInfo.getUploadDate(),
                        fileInfo.getFileName(),
                        fileInfo.getContentType(),
                        fileInfo.getMetadata()
                );

        return session.executeAsync(insert)
                .then(rs -> {
                    context.stop();
                    return null;
                }).otherwise(t -> {
                    context.error();
                    return when.reject(t);
                });

    }

    @Override
    public Promise<Void> storeChunk(ChunkInfo chunkInfo) {

        final Metrics.Context context = chunkMetrics.timeWrite();

        BoundStatement insert = statements
                .getStoreChunk()
                .bind(
                        chunkInfo.getId(),
                        chunkInfo.getNum(),
                        ByteBuffer.wrap(chunkInfo.getData())
                );

        return session.executeAsync(insert)
                .then(rs -> {
                    context.stop();
                    return null;
                })
                .otherwise(t -> {
                    context.error();
                    return when.reject(t);
                });

    }

    @Override
    public Promise<FileInfo> loadFile(final UUID id) {

        Metrics.Context context = fileMetrics.timeRead();
        BoundStatement select = statements.getLoadFile().bind(id);

        return session.executeAsync(select)
                .then(result -> {
                    Row row = result.one();

                    if (row == null) {
                        context.stop();
                        return when.resolve(null);
                    }

                    DefaultFileInfo fileInfo = new DefaultFileInfo()
                            .setId(id)
                            .setFileName(row.getString("filename"))
                            .setContentType(row.getString("contentType"))
                            .setLength(row.getLong("length"))
                            .setChunkSize(row.getInt("chunkSize"))
                            .setUploadDate(row.getLong("uploadDate"))
                            .setMetadata(row.getMap("metadata", String.class, String.class));

                    context.stop();
                    return when.resolve(fileInfo);

                })
                .otherwise(t -> {
                    context.error();
                    return when.reject(t);
                });

    }

    @Override
    public Promise<ChunkInfo> loadChunk(final UUID id, final int n) {

        Metrics.Context context = chunkMetrics.timeRead();
        BoundStatement select = statements.getLoadChunk().bind(id, n);

        return session.executeAsync(select)
                .then(result -> {
                    Row row = result.one();

                    if (row == null) {
                        context.stop();
                        return when.resolve(null);
                    }

                    ByteBuffer bb = row.getBytes("data");
                    byte[] data = new byte[bb.remaining()];
                    bb.get(data);

                    DefaultChunkInfo chunkInfo = new DefaultChunkInfo()
                            .setId(id)
                            .setNum(n)
                            .setData(data);

                    context.stop();
                    return when.resolve(chunkInfo);

                })
                .otherwise(t -> {
                    context.error();
                    return when.reject(t);
                });

    }
}
