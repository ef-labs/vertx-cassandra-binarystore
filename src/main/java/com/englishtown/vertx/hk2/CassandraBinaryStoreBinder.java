package com.englishtown.vertx.hk2;

import com.datastax.driver.core.Cluster;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

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

        install(new MetricsBinder());

        bind(Cluster.Builder.class).to(Cluster.Builder.class);

    }
}
