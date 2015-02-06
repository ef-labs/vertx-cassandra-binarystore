//package com.englishtown.vertx.cassandra.binarystore;
//
//import io.vertx.core.AbstractVerticle;
//import io.vertx.core.Future;
//import io.vertx.core.buffer.Buffer;
//import io.vertx.core.json.JsonObject;
//import io.vertx.serviceproxy.ProxyHelper;
//
//import javax.inject.Inject;
//import java.util.HashMap;
//import java.util.Map;
//
///**
// * Created by adriangonzalez on 1/20/15.
// */
//public class BinaryStoreVerticle extends AbstractVerticle {
//
//    private final CassandraBinaryStoreService service;
//
//    @Inject
//    public BinaryStoreVerticle(CassandraBinaryStoreService service) {
//        this.service = service;
//    }
//
//    @Override
//    public void start(Future<Void> startFuture) throws Exception {
//
//        String address = config().getString("address");
//        if (address == null || address.isEmpty()) {
//            throw new IllegalStateException("address field must be specified in config for service verticle");
//        }
//
//        // Register service as an event bus proxy
//        ProxyHelper.registerService(CassandraBinaryStoreService.class, vertx, service, address);
//
//        // Start the service
//        service.start(startFuture);
//
//    }
//
////    private void init() {
////
////        eb = vertx.eventBus();
////
////        JsonObject config = config();
////        address = config.getString("address", DEFAULT_ADDRESS);
////
////        // Main Message<JsonObject> handler that inspects an "action" field
////        eb.consumer(address, this);
////
////        // Message<Buffer> handler to save file chunks
////        eb.<Buffer>consumer(address + "/saveChunk", message -> {
////            messageCounter.inc();
////            saveChunk(message);
////        });
////
////        // Start jmx metric reporter if enabled
////        Map<String, Object> values = new HashMap<>();
////        values.put("address", address);
//////        reporter = com.englishtown.vertx.metrics.Utils.create(this, registry, values, config);
////
////    }
//
//    @Override
//    public void stop() throws Exception {
//        service.stop();
//    }
//
//}
