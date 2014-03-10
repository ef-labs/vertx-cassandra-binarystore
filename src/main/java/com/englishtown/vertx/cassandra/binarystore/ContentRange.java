package com.englishtown.vertx.cassandra.binarystore;

/**
 * Content byte[] range
 */
public interface ContentRange {

    /**
     * Range start index (zero-based)
     *
     * @return
     */
    long getFrom();

    /**
     * Range end index (inclusive)
     *
     * @return
     */
    long getTo();

}
