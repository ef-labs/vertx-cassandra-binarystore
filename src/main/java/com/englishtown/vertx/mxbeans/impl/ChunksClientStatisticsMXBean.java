package com.englishtown.vertx.mxbeans.impl;

import com.englishtown.jmx.BeanManager;
import com.englishtown.jmx.defaults.DefaultPersistorStatisticsMXBean;
import org.apache.commons.math.stat.descriptive.SynchronizedSummaryStatistics;

import java.util.Hashtable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Defined as an enum to be a Singleton within the JVM, as if we have multiple instances of this verticle running we need them to all be
 * referencing the same instance of this class.
 */
public enum ChunksClientStatisticsMXBean implements DefaultPersistorStatisticsMXBean {
    INSTANCE;

    private AtomicLong recordsWritten = new AtomicLong();
    private AtomicLong writeErrors = new AtomicLong();

    private AtomicLong recordsRead = new AtomicLong();
    private AtomicLong readErrors = new AtomicLong();

    private SynchronizedSummaryStatistics readStatistics = new SynchronizedSummaryStatistics();
    private SynchronizedSummaryStatistics writeStatistics = new SynchronizedSummaryStatistics();

    private ChunksClientStatisticsMXBean() {
        final Hashtable<String, String> chunksStatsBeanKeys = new Hashtable<>();
        chunksStatsBeanKeys.put("subtype", "chunks");
        chunksStatsBeanKeys.put("type", "PersistorStatistics");
        try {
            BeanManager.INSTANCE.registerBean(this, chunksStatsBeanKeys);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public long getRecordsWritten() {
        return recordsWritten.get();
    }

    @Override
    public long getRecordsRead() {
        return recordsRead.get();
    }

    @Override
    public double getSlowestWriteTime() {
        return writeStatistics.getMax();
    }

    @Override
    public double getFastestWriteTime() {
        return writeStatistics.getMin();
    }

    @Override
    public double getSlowestReadTime() {
        return readStatistics.getMax();
    }

    @Override
    public double getFastestReadTime() {
        return readStatistics.getMin();
    }

    @Override
    public long getReadErrors() {
        return readErrors.get();
    }

    @Override
    public long getWriteErrors() {
        return writeErrors.get();
    }

    @Override
    public double getWriteMean() {
        return writeStatistics.getMean();
    }

    @Override
    public double getWriteStandardDeviation() {
        return writeStatistics.getStandardDeviation();
    }

    @Override
    public double getReadMean() {
        return readStatistics.getMean();
    }

    @Override
    public void reset() {
        readStatistics.clear();
        writeStatistics.clear();

        recordsRead.set(0);
        readErrors.set(0);
        recordsWritten.set(0);
        writeErrors.set(0);
    }

    @Override
    public double getReadStandardDeviation() {
        return readStatistics.getStandardDeviation();
    }

    public void incrementWriteErrorCount() {
        writeErrors.getAndIncrement();
    }

    public void incrementReadErrorCount() {
        readErrors.getAndIncrement();
    }

    public void addToReadStats(long milliseconds) {
        recordsRead.getAndIncrement();
        readStatistics.addValue(milliseconds);
    }

    public void addToWriteStats(long milliseconds) {
        recordsWritten.getAndIncrement();
        writeStatistics.addValue(milliseconds);
    }
}
