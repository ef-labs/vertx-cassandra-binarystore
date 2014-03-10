package com.englishtown.integration.java;

import com.englishtown.promises.FulfilledRunnable;
import com.englishtown.promises.Promise;
import com.englishtown.promises.RejectedRunnable;
import com.englishtown.promises.Value;
import com.englishtown.vertx.cassandra.binarystore.BinaryStoreStarter;
import com.englishtown.vertx.jersey.JerseyServer;
import com.englishtown.vertx.jersey.promises.WhenJerseyServer;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.platform.Verticle;

import javax.inject.Inject;

/**
 * Created by adriangonzalez on 3/9/14.
 */
public class TestContentEndpointVerticle extends Verticle {

    private final WhenJerseyServer whenJerseyServer;
    private BinaryStoreStarter starter;

    @Inject
    public TestContentEndpointVerticle(WhenJerseyServer whenJerseyServer, BinaryStoreStarter starter) {
        this.whenJerseyServer = whenJerseyServer;
        this.starter = starter;
    }

    @Override
    public void start(final Future<Void> startedResult) {

        whenJerseyServer.createServer(container.config())
                .then(
                        new FulfilledRunnable<JerseyServer>() {
                            @Override
                            public Promise<JerseyServer> run(JerseyServer jerseyServer) {
                                starter.run(new Handler<AsyncResult<Void>>() {
                                    @Override
                                    public void handle(AsyncResult<Void> result) {
                                        if (result.succeeded()) {
                                            startedResult.setResult(null);
                                        } else {
                                            startedResult.setFailure(result.cause());
                                        }
                                    }
                                });
                                return null;
                            }
                        },
                        new RejectedRunnable<JerseyServer>() {
                            @Override
                            public Promise<JerseyServer> run(Value<JerseyServer> value) {
                                startedResult.setFailure(value.getCause());
                                return null;
                            }
                        }
                );

    }

}
