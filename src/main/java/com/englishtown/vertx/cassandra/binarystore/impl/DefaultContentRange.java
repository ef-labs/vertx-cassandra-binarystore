package com.englishtown.vertx.cassandra.binarystore.impl;

import com.englishtown.vertx.cassandra.binarystore.ContentRange;

import java.util.Objects;

/**
 * Created by adriangonzalez on 3/10/14.
 */
public class DefaultContentRange implements ContentRange {

    long from;
    long to;

    /**
     * Range start index (zero-based)
     *
     * @return
     */
    @Override
    public long getFrom() {
        return from;
    }

    public DefaultContentRange setFrom(long from) {
        this.from = from;
        return this;
    }

    /**
     * Range end index (inclusive)
     *
     * @return
     */
    @Override
    public long getTo() {
        return to;
    }

    public DefaultContentRange setTo(long to) {
        this.to = to;
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (obj == this) return true;

        if (!(obj instanceof DefaultContentRange)) return false;

        DefaultContentRange other = (DefaultContentRange) obj;

        if (!Objects.equals(this.getFrom(), other.getFrom())) return false;
        if (!Objects.equals(this.getTo(), other.getTo())) return false;

        return true;
    }
}
