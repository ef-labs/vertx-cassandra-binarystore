package com.englishtown.vertx.cassandra.binarystore.resources;

import com.google.common.primitives.Ints;

import java.util.Arrays;

/**
 * Represents an http GET range request
 */
class ContentRange {

    private final int startChunk;
    private final int endChunk;
    private final int startPos;
    private final int endPos;

    private final long from;
    private final long to;


    public ContentRange(long from, long to, int chunkSize) throws IllegalArgumentException {
        startChunk = Ints.checkedCast(from / chunkSize);
        endChunk = Ints.checkedCast(to / chunkSize);

        startPos = Ints.checkedCast(from - (startChunk * chunkSize));
        endPos = Ints.checkedCast(to - (endChunk * chunkSize));

        this.from = from;
        this.to = to;
    }

    public byte[] getRequiredBytesFromChunk(int chunkNumber, byte[] chunk) {
        if (chunkNumber == startChunk) {
            return Arrays.copyOfRange(chunk, startPos, chunk.length - 1);
        }

        if (chunkNumber == endChunk) {
            return Arrays.copyOfRange(chunk, 0, endPos);
        }

        return chunk;
    }

    public int getStartChunk() {
        return startChunk;
    }

    public int getEndChunk() {
        return endChunk;
    }

    public long getFrom() {
        return from;
    }

    public long getTo() {
        return to;
    }
}
