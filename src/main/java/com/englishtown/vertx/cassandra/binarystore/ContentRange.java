package com.englishtown.vertx.cassandra.binarystore;

import java.util.Objects;

/**
 * Created by adriangonzalez on 3/10/14.
 */
public class ContentRange {

    long from;
    long to;

    /**
     * Range start index (zero-based)
     *
     * @return
     */
    public long getFrom() {
        return from;
    }

    public ContentRange setFrom(long from) {
        this.from = from;
        return this;
    }

    /**
     * Range end index (inclusive)
     *
     * @return
     */
    public long getTo() {
        return to;
    }

    public ContentRange setTo(long to) {
        this.to = to;
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (obj == this) return true;

        if (!(obj instanceof ContentRange)) return false;

        ContentRange other = (ContentRange) obj;

        if (!Objects.equals(this.getFrom(), other.getFrom())) return false;
        if (!Objects.equals(this.getTo(), other.getTo())) return false;

        return true;
    }
}
