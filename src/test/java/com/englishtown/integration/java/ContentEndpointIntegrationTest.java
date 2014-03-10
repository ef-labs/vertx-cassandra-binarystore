package com.englishtown.integration.java;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.io.*;

import static org.vertx.testtools.VertxAssert.*;
import static org.vertx.testtools.VertxAssert.fail;
import static org.vertx.testtools.VertxAssert.testComplete;

/**
 * Created by adriangonzalez on 3/9/14.
 */
public class ContentEndpointIntegrationTest extends TestVerticle {

    /**
     * {@inheritDoc}
     */
    @Override
    public void start(final Future<Void> startedResult) {

        // Initialize integration verticle
        this.initialize();

        JsonObject config = new JsonObject()
                .putString("host", "localhost")
                .putNumber("port", 8080)
                .putArray("resources", new JsonArray()
                        .addString("com.englishtown.vertx.cassandra.binarystore.resources")
                )
                .putArray("hk2_binder", new JsonArray()
                        .addString("com.englishtown.vertx.jersey.inject.VertxJerseyBinder")
                        .addString("com.englishtown.vertx.hk2.CassandraBinaryStoreBinder")
                        .addString("com.englishtown.vertx.hk2.WhenJerseyBinder")
                );

        container.deployVerticle(TestContentEndpointVerticle.class.getName(), config, new Handler<AsyncResult<String>>() {
            @Override
            public void handle(AsyncResult<String> result) {
                if (result.succeeded()) {
                    startedResult.setResult(null);
                    start();
                } else {
                    startedResult.setFailure(result.cause());
                    handleThrowable(result.cause());
                    fail();
                }
            }
        });

    }

//    private JsonObject loadConfig() {
//
//        try (InputStream stream = this.getClass().getResourceAsStream("/config.json")) {
//            StringBuilder sb = new StringBuilder();
//            BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
//
//            String line = reader.readLine();
//            while (line != null) {
//                sb.append(line).append('\n');
//                line = reader.readLine();
//            }
//
//            return new JsonObject(sb.toString());
//
//        } catch (IOException e) {
//            e.printStackTrace();
//            fail();
//            return new JsonObject();
//        }
//
//    }

    private HttpClient createHttpClient() {

        HttpClient client = vertx.createHttpClient();

        client.setHost("localhost")
                .setPort(8080)
                .setConnectTimeout(500)
                .exceptionHandler(new Handler<Throwable>() {
                    @Override
                    public void handle(Throwable t) {
                        fail(t.getMessage());
                    }
                });

        return client;
    }

    @Test
    public void testMediaResource_Upload_And_Read() throws Exception {

        final byte[] image = loadImage();
        HttpClient client = createHttpClient();

        HttpClientRequest request = client.post("/content", new Handler<HttpClientResponse>() {
            @Override
            public void handle(HttpClientResponse response) {
                assertEquals(200, response.statusCode());
                assertEquals(MediaType.APPLICATION_JSON, response.headers().get(HttpHeaders.CONTENT_TYPE));

                response.bodyHandler(new Handler<Buffer>() {
                    @Override
                    public void handle(Buffer body) {
                        JsonObject json = new JsonObject(body.toString("UTF-8"));
                        String id = json.getString("id");
                        testMediaResource_Get_Image(id, image);
                    }
                });
            }
        });

        writeBody(request, image);

    }

    private void testMediaResource_Get_Image(String id, final byte[] image) {

        HttpClient client = createHttpClient();

        HttpClientRequest request = client.get("/content?id=" + id, new Handler<HttpClientResponse>() {
            @Override
            public void handle(HttpClientResponse response) {
                assertEquals(200, response.statusCode());
                assertEquals("image/jpeg", response.headers().get(HttpHeaders.CONTENT_TYPE));
                response.bodyHandler(new Handler<Buffer>() {
                    @Override
                    public void handle(Buffer body) {
                        assertArrayEquals(image, body.getBytes());
                        testComplete();
                    }
                });
            }
        });

        request.end();

    }

    private void writeBody(HttpClientRequest request, byte[] image) {
        final String boundary = "----dLV9Wyq26L_-JQxk6ferf-RT153LhOO";
        Buffer buffer = new Buffer();

        buffer.appendString("--" + boundary + "\r\n" +
                "Content-Disposition: form-data; name=\"file\"; filename=\"test-image.jpg\"\r\n" +
                "Content-Type: image/jpeg\r\n" +
                "\r\n", "UTF-8");

        buffer.appendBytes(image);

        buffer.appendString("\r\n" +
                "--" + boundary + "--\r\n", "UTF-8");

        request.headers().set(HttpHeaders.CONTENT_LENGTH, String.valueOf(buffer.length()));
        request.headers().set(HttpHeaders.CONTENT_TYPE, MediaType.MULTIPART_FORM_DATA + "; boundary=" + boundary);
        request.write(buffer).end();
    }

    private byte[] loadImage() throws Exception {
        File file = new File(this.getClass().getClassLoader().getResource("EF_Labs_ENG_logo.JPG").toURI());
        return Files.toByteArray(file);
    }

}
