package com.englishtown.vertx.mxbeans;

/**
 */
public interface ClientStatisticsMXBean {
    public long getRecordsWritten();

    public double getSlowestWriteTime();

    public double getFastestWriteTime();

    public double getWriteMean();

    public double getWriteStandardDeviation();


    public long getRecordsRead();

    public double getSlowestReadTime();

    public double getFastestReadTime();

    public double getReadMean();

    public double getReadStandardDeviation();


    public long getReadErrors();

    public long getWriteErrors();
}
