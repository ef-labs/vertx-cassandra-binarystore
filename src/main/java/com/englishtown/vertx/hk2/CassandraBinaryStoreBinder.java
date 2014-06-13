package com.englishtown.vertx.hk2;

import com.englishtown.vertx.cassandra.binarystore.*;
import com.englishtown.vertx.cassandra.binarystore.impl.DefaultBinaryStoreManager;
import com.englishtown.vertx.cassandra.binarystore.impl.DefaultBinaryStoreReader;
import com.englishtown.vertx.cassandra.binarystore.impl.DefaultBinaryStoreStatements;
import com.englishtown.vertx.cassandra.binarystore.impl.DefaultBinaryStoreWriter;
import com.englishtown.vertx.cassandra.hk2.WhenCassandraSessionBinder;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

import javax.inject.Singleton;

/**
 *
 */
public class CassandraBinaryStoreBinder extends AbstractBinder {

    /**
     * Implement to provide binding definitions using the exposed binding
     * methods.
     */
    @Override
    protected void configure() {

        install(new MetricsBinder(), new WhenCassandraSessionBinder());

        bind(BinaryStoreStarter.class).to(BinaryStoreStarter.class);
        bind(DefaultBinaryStoreManager.class).to(BinaryStoreManager.class).in(Singleton.class);
        bind(DefaultBinaryStoreStatements.class).to(BinaryStoreStatements.class).in(Singleton.class);
        bind(DefaultBinaryStoreWriter.class).to(BinaryStoreWriter.class).in(Singleton.class);
        bind(DefaultBinaryStoreReader.class).to(BinaryStoreReader.class).in(Singleton.class);

    }
}
