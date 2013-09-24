package com.englishtown.vertx.mxbeans;

import com.englishtown.jmx.defaults.DefaultVerticleMXBean;

/**
 */
public interface CassandraGeneralInfoMXBean extends DefaultVerticleMXBean {
    public String getIPs();
}
