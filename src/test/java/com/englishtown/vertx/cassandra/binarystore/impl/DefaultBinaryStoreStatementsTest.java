package com.englishtown.vertx.cassandra.binarystore.impl;

import com.datastax.driver.core.PreparedStatement;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;

/**
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultBinaryStoreStatementsTest {

    @Mock
    PreparedStatement storeChunk;
    @Mock
    PreparedStatement storeFile;
    @Mock
    PreparedStatement loadChunk;
    @Mock
    PreparedStatement loadFile;

    String keyspace = "test.keyspace";

    @Test
    public void simpleStatementsRetentionTest() throws Exception {

        // Initialise
        DefaultBinaryStoreStatements dbss = new DefaultBinaryStoreStatements();

        // When we set our various fields on DefaultBinaryStoreStatements
        dbss.setKeyspace(keyspace);
        dbss.setLoadChunk(loadChunk);
        dbss.setLoadFile(loadFile);
        dbss.setStoreChunk(storeChunk);
        dbss.setStoreFile(storeFile);

        // Then we expect the get methods to return the same objects
        assertEquals(keyspace, dbss.getKeyspace());
        assertEquals(loadChunk, dbss.getLoadChunk());
        assertEquals(loadFile, dbss.getLoadFile());
        assertEquals(storeChunk, dbss.getStoreChunk());
        assertEquals(storeFile, dbss.getStoreFile());
    }
}
