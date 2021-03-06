package com.englishtown.vertx.cassandra.binarystore.impl;

import com.englishtown.vertx.cassandra.binarystore.FileInfo;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by adriangonzalez on 2/12/14.
 */
public class FileInfoTest {
    @Test
    public void testGetChunkCount() throws Exception {

        FileInfo fileInfo = new FileInfo();
        int count;

        count = fileInfo.getChunkCount();
        assertEquals(0, count);

        fileInfo.setLength(500);
        count = fileInfo.getChunkCount();
        assertEquals(0, count);

        fileInfo.setChunkSize(500);
        count = fileInfo.getChunkCount();
        assertEquals(1, count);

        fileInfo.setChunkSize(400);
        count = fileInfo.getChunkCount();
        assertEquals(2, count);

        fileInfo.setChunkSize(200);
        count = fileInfo.getChunkCount();
        assertEquals(3, count);

        fileInfo.setChunkSize(1000);
        count = fileInfo.getChunkCount();
        assertEquals(1, count);

    }
}
