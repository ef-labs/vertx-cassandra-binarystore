package com.englishtown.vertx.cassandra.binarystore;

import com.englishtown.promises.*;
import com.englishtown.vertx.cassandra.CassandraSession;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BinaryStoreStarterTest {

    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();
    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Mock
    private CassandraSession session;
    @Mock
    private BinaryStoreStatements statements;
    @Mock
    private Vertx vertx;
    @Mock
    private Context context;

    private JsonObject config = new JsonObject();
    private When when = WhenFactory.createSync();

    @InjectMocks
    private BinaryStoreStarter starter;

    @Before
    public void setUp() {
        when(vertx.getOrCreateContext()).thenReturn(context);
        when(context.config()).thenReturn(config);
    }

    @Test
    public void run() {
        run(BinaryStoreStarter.DEFAULT_KEYSPACE);
    }

    @Test
    public void run_config() {
        String keyspace = "config_keyspace";
        config.put("keyspace", keyspace);
        run(keyspace);
    }

    @Test
    public void run_env() {
        String keyspace = "env_keyspace";
        environmentVariables.set(BinaryStoreStarter.ENV_VAR_KEYSPACE, keyspace);
        run(keyspace);
    }

    private void run(String expectedKeyspace) {

        when(statements.init(anyString())).thenReturn(when.resolve(null));

        Promise<?> p = starter.run();

        verify(statements).init(eq(expectedKeyspace));

        assertNotNull(p);
        State<?> state = p.inspect();
        assertEquals(HandlerState.FULFILLED, state.getState());

    }

    @Test
    public void close() throws Exception {
        starter.close();
        verify(session).close();
    }
}