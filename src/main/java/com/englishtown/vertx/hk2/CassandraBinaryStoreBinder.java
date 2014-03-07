package com.englishtown.vertx.hk2;

import com.englishtown.vertx.cassandra.binarystore.BinaryStoreManager;
import com.englishtown.vertx.cassandra.binarystore.BinaryStoreStarter;
import com.englishtown.vertx.cassandra.binarystore.BinaryStoreStatements;
import com.englishtown.vertx.cassandra.binarystore.BinaryStoreWriter;
import com.englishtown.vertx.cassandra.binarystore.impl.DefaultBinaryStoreManager;
import com.englishtown.vertx.cassandra.binarystore.impl.DefaultBinaryStoreStatements;
import com.englishtown.vertx.cassandra.binarystore.impl.DefaultBinaryStoreWriter;
import com.englishtown.vertx.cassandra.hk2.CassandraSessionBinder;
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

        install(new MetricsBinder(), new CassandraSessionBinder());

        bind(BinaryStoreStarter.class).to(BinaryStoreStarter.class);
        bind(DefaultBinaryStoreManager.class).to(BinaryStoreManager.class).in(Singleton.class);
        bind(DefaultBinaryStoreStatements.class).to(BinaryStoreStatements.class).in(Singleton.class);
        bind(DefaultBinaryStoreWriter.class).to(BinaryStoreWriter.class).in(Singleton.class);

    }
}
