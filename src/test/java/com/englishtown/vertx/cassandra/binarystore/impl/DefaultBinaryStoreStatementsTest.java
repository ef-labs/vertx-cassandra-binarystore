package com.englishtown.vertx.cassandra.binarystore.impl;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;
import com.englishtown.promises.FulfilledRunnable;
import com.englishtown.promises.Promise;
import com.englishtown.promises.RejectedRunnable;
import com.englishtown.vertx.cassandra.promises.WhenCassandraSession;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

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
    @Mock
    WhenCassandraSession session;
    @Mock
    Cluster cluster;
    @Mock
    Configuration configuration;
    @Mock
    Policies policies;
    @Mock
    LoadBalancingPolicy lbPolicy;
    @Mock
    FutureCallback<Void> callback;
    @Mock
    Metadata metadata;
    @Mock
    Promise<ResultSet> resultSetPromise;
    @Captor
    ArgumentCaptor<FulfilledRunnable<ResultSet>> fulfilledCaptor;
    @Captor
    ArgumentCaptor<RejectedRunnable<ResultSet>> rejectedCaptor;

    String keyspace = "test.keyspace";

    @Before
    public void setUp() throws Exception {
        when(session.getMetadata()).thenReturn(metadata);
        when(session.executeAsync(any(RegularStatement.class))).thenReturn(resultSetPromise).thenReturn(null);
        when(session.getCluster()).thenReturn(cluster);
        when(cluster.getConfiguration()).thenReturn(configuration);
        when(configuration.getPolicies()).thenReturn(policies);
        when(policies.getLoadBalancingPolicy()).thenReturn(lbPolicy);
        when(lbPolicy.distance(any(Host.class))).thenReturn(HostDistance.LOCAL);
    }

    @Test
    public void simpleStatementsRetentionTest() throws Exception {

        // Initialise
        DefaultBinaryStoreStatements dbss = new DefaultBinaryStoreStatements(session);

        // When we set our various fields on DefaultBinaryStoreStatements
        dbss.init(keyspace, callback);
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


    @Test
    public void testInit() throws Exception {

        DefaultBinaryStoreStatements dbss = new DefaultBinaryStoreStatements(session);
        assertFalse(dbss.isInitialized());

        dbss.init(keyspace, callback);

        verify(resultSetPromise).then(fulfilledCaptor.capture(), rejectedCaptor.capture());
        fulfilledCaptor.getValue().run(null);

        verify(session, times(3)).executeAsync(any(SimpleStatement.class));
        verify(session, times(4)).prepareAsync(any(RegularStatement.class));
        assertTrue(dbss.isInitialized());

    }

}
