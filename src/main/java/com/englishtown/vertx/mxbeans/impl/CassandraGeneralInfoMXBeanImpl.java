package com.englishtown.vertx.mxbeans.impl;

import com.englishtown.vertx.mxbeans.CassandraGeneralInfoMXBean;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.Arrays;

/**
 */
public class CassandraGeneralInfoMXBeanImpl implements CassandraGeneralInfoMXBean {
    private String IPs;
    private String address;
    private String config;
    private boolean worker;

    public CassandraGeneralInfoMXBeanImpl(String address, JsonObject config, boolean worker, JsonArray IPs) {
        this.IPs = Arrays.toString(IPs.toArray());
        this.address = address;
        this.config = config.toString();
        this.worker = worker;
    }

    @Override
    public String getIPs() {
        return IPs;
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    public String getConfig() {
        return config;
    }

    @Override
    public boolean isWorker() {
        return worker;
    }
}
