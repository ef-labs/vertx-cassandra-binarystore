package com.englishtown.vertx.mxbeans.impl;

import com.englishtown.jmx.defaults.DefaultPersistorStatisticsMXBean;
import org.apache.commons.math.stat.descriptive.SynchronizedSummaryStatistics;

import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class ClientStatisticsMXBeanImpl implements DefaultPersistorStatisticsMXBean {
    private AtomicLong recordsWritten = new AtomicLong();
    private AtomicLong writeErrors = new AtomicLong();

    private AtomicLong recordsRead = new AtomicLong();
    private AtomicLong readErrors = new AtomicLong();

    private SynchronizedSummaryStatistics readStatistics = new SynchronizedSummaryStatistics();
    private SynchronizedSummaryStatistics writeStatistics = new SynchronizedSummaryStatistics();

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

    public void incrementWriteCount() {
        recordsWritten.getAndIncrement();
    }

    public void incremementWriteErrorCount() {
        writeErrors.getAndIncrement();
    }

    public void incrementReadCount() {
        recordsRead.getAndIncrement();
    }

    public void incremementReadErrorCount() {
        readErrors.getAndIncrement();
    }

    public void addToReadStats(long milliseconds) {
        readStatistics.addValue(milliseconds);
    }

    public void addToWriteStats(long milliseconds) {
        writeStatistics.addValue(milliseconds);
    }
}
