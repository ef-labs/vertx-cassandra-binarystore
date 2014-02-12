package com.englishtown.vertx.hk2;

import com.englishtown.vertx.cassandra.hk2.CassandraSessionBinder;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

/**
 *
 */
public class BootstrapBinder extends AbstractBinder {

    /**
     * Implement to provide binding definitions using the exposed binding
     * methods.
     */
    @Override
    protected void configure() {

        install(new CassandraBinaryStoreBinder(), new CassandraSessionBinder());

    }
}
