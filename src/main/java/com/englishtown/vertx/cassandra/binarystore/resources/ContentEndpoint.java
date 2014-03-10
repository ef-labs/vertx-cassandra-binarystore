package com.englishtown.vertx.cassandra.binarystore.resources;

import com.englishtown.vertx.cassandra.binarystore.BinaryStoreManager;
import com.englishtown.vertx.cassandra.binarystore.BinaryStoreWriter;
import com.englishtown.vertx.cassandra.binarystore.ChunkInfo;
import com.englishtown.vertx.cassandra.binarystore.FileInfo;
import com.englishtown.vertx.cassandra.binarystore.impl.DefaultFileInfo;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.FutureCallback;
import org.glassfish.jersey.server.ChunkedOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServerFileUpload;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonObject;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.UUID;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static javax.ws.rs.core.Response.ResponseBuilder;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.PARTIAL_CONTENT;

/**
 * Created by adriangonzalez on 2/12/14.
 */
@Singleton
@Path("content")
public class ContentEndpoint {

    private final BinaryStoreManager binaryStoreManager;
    private final BinaryStoreWriter binaryStoreWriter;

    private static final Logger logger = LoggerFactory.getLogger(ContentEndpoint.class);
    public static final int DEFAULT_CHUNK_SIZE = 1024000;

    @Inject
    public ContentEndpoint(BinaryStoreManager binaryStoreManager, BinaryStoreWriter binaryStoreWriter) {
        this.binaryStoreManager = binaryStoreManager;
        this.binaryStoreWriter = binaryStoreWriter;
    }

    @HEAD
    public void doHead(
            @QueryParam("id") final String id,
            @Suspended final AsyncResponse asyncResponse
    ) {

        UUID uuid = getId(id, asyncResponse);
        if (uuid == null) {
            return;
        }

        doHead(uuid, asyncResponse);
    }

    public void doHead(
            UUID id,
            final AsyncResponse asyncResponse
    ) {

        binaryStoreManager.loadFile(id, new FutureCallback<FileInfo>() {
            @Override
            public void onSuccess(FileInfo fileInfo) {
                if (fileInfo == null) {
                    asyncResponse.resume(Response.status(NOT_FOUND).build());
                } else {
                    ResponseBuilder responseBuilder = Response.ok();
                    responseBuilder.header(ACCEPT_RANGES, "bytes");
                    responseBuilder.header(CONTENT_TYPE, fileInfo.getContentType());
                    responseBuilder.header(CONTENT_LENGTH, fileInfo.getLength());
                    asyncResponse.resume(responseBuilder.build());
                }
            }

            @Override
            public void onFailure(Throwable t) {
                handleException(asyncResponse, "Error while reading file on HEAD request", t);
            }
        });

    }

    @GET
    public void doGet(
            @QueryParam("id") String id,
            @HeaderParam("Range") final String range,
            @Suspended final AsyncResponse asyncResponse
    ) {

        UUID uuid = getId(id, asyncResponse);
        if (uuid == null) {
            return;
        }

        doGet(uuid, range, asyncResponse);
    }

    public void doGet(
            UUID id,
            final String range,
            final AsyncResponse asyncResponse) {

        binaryStoreManager.loadFile(id, new FutureCallback<FileInfo>() {
            @Override
            public void onSuccess(FileInfo fileInfo) {
                if (fileInfo == null) {
                    asyncResponse.resume(Response
                            .status(NOT_FOUND)
                            .build());
                } else {
                    if (Strings.isNullOrEmpty(range)) {
                        // Do the chunked response of the whole content
                        final ChunkedOutput<byte[]> chunkedOutput = new ChunkedOutput<>(byte[].class);

                        ResponseBuilder builder = Response
                                .ok(chunkedOutput)
                                .header(CONTENT_TYPE, fileInfo.getContentType())
                                .header(ACCEPT_RANGES, "bytes");

                        if ("application/zip".equals(fileInfo.getContentType())) {
                            builder.header("Content-Disposition", "attachment; filename=" + fileInfo.getFileName());
                        }

                        asyncResponse.resume(builder.build());
                        loadChunks(0, fileInfo.getChunkCount(), fileInfo, chunkedOutput);

                    } else {
                        // Or do the range response as specified by the header
                        Buffer buffer = new Buffer();

                        String[] ranges = range.split("=")[1].split("-");
                        long from = Long.parseLong(ranges[0]);
                        long to = fileInfo.getLength() - 1;
                        if (ranges.length == 2) {
                            to = Integer.parseInt(ranges[1]);
                        }

                        ContentRange contentRange = new ContentRange(from, to, fileInfo.getChunkSize());
                        loadRangeChunks(contentRange.getStartChunk(), contentRange, fileInfo, buffer, asyncResponse);
                    }
                }
            }

            @Override
            public void onFailure(Throwable t) {
                handleException(asyncResponse, "Error loading file info", t);
            }
        });

    }

    @POST()
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    public void doPost(
            @Suspended final AsyncResponse asyncResponse,
            @Context HttpServerRequest vertxRequest
    ) throws Exception {
        doPost(asyncResponse, vertxRequest, DEFAULT_CHUNK_SIZE);
    }

    public void doPost(
            final AsyncResponse asyncResponse,
            final HttpServerRequest vertxRequest,
            final int chunkSize
    ) throws Exception {

        vertxRequest.expectMultiPart(true);

        vertxRequest.uploadHandler(new Handler<HttpServerFileUpload>() {
            @Override
            public void handle(final HttpServerFileUpload fileUpload) {

                FileInfo fileInfo = new DefaultFileInfo()
                        .setFileName(fileUpload.filename())
                        .setContentType(fileUpload.contentType())
                        .setUploadDate(System.currentTimeMillis())
                        .setChunkSize(chunkSize);

                binaryStoreWriter.write(fileInfo, fileUpload, new FutureCallback<FileInfo>() {
                    @Override
                    public void onSuccess(FileInfo fileInfo) {
                        JsonObject json = new JsonObject().putString("id", fileInfo.getId().toString());
                        asyncResponse.resume(Response.ok(json.encode()).build());
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        handleException(asyncResponse, "Error uploading the file", t);
                    }
                });


            }
        });

    }

    private UUID getId(String id, AsyncResponse asyncResponse) {

        try {
            return UUID.fromString(id);

        } catch (Throwable t) {
            logger.error("id is null or invalid UUID: ", id);
            asyncResponse.resume(Response.status(Response.Status.BAD_REQUEST).build());
            return null;
        }

    }

    private void loadChunks(final int n, final int count, final FileInfo fileInfo, final ChunkedOutput<byte[]> chunkedOutput) {

        if (n == count) {
            try {
                chunkedOutput.close();
            } catch (IOException e) {
                // Squash exception if connection is closed
            }
            return;
        }

        binaryStoreManager.loadChunk(fileInfo.getId(), n, new FutureCallback<ChunkInfo>() {
            @Override
            public void onSuccess(ChunkInfo chunkInfo) {
                try {
                    if (chunkInfo != null) {
                        chunkedOutput.write(chunkInfo.getData());
                        loadChunks(n + 1, count, fileInfo, chunkedOutput);
                    } else {
                        chunkedOutput.close();
                    }
                } catch (IOException e) {
                    // Squash exception if connection is closed
                    return;
                }

            }

            @Override
            public void onFailure(Throwable t) {
                logger.error("Error loading chunk", t);
                try {
                    chunkedOutput.close();
                } catch (IOException e) {
                    // Squash exception if connection is closed
                }
            }
        });
    }

    private void loadRangeChunks(
            final int n,
            final ContentRange contentRange,
            final FileInfo fileInfo,
            final Buffer buffer,
            final AsyncResponse asyncResponse
    ) {

        if (n > contentRange.getEndChunk()) {
            sendRangeResponse(fileInfo, contentRange, buffer, asyncResponse);
        } else {
            binaryStoreManager.loadChunk(fileInfo.getId(), n, new FutureCallback<ChunkInfo>() {
                @Override
                public void onSuccess(ChunkInfo chunkInfo) {
                    if (chunkInfo == null) {
                        Throwable t = new Throwable("Error while reading chunk " + n + ". It came back as null.");
                        handleException(asyncResponse, t.getMessage(), t);
                    } else {
                        buffer.appendBytes(contentRange.getRequiredBytesFromChunk(n, chunkInfo.getData()));
                        loadRangeChunks(n + 1, contentRange, fileInfo, buffer, asyncResponse);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    handleException(asyncResponse, "Error while reading chunk " + n, t);
                }
            });
        }
    }

    private void sendRangeResponse(FileInfo fileInfo, ContentRange contentRange, Buffer buffer, AsyncResponse asyncResponse) {
        ResponseBuilder responseBuilder = Response.status(PARTIAL_CONTENT);
        responseBuilder.header(CONTENT_RANGE, "bytes " + contentRange.getFrom() + "-" + contentRange.getTo() + "/" + fileInfo.getLength());
        responseBuilder.header(ACCEPT_RANGES, "bytes");
        responseBuilder.header(CONTENT_LENGTH, (contentRange.getTo() - contentRange.getFrom()) + 1);
        responseBuilder.header(CONTENT_TYPE, fileInfo.getContentType());

        responseBuilder.entity(buffer.getBytes());

        asyncResponse.resume(responseBuilder.build());
    }

    private void handleException(AsyncResponse asyncResponse, String message, Throwable e) {
        logger.error(message, e);
        asyncResponse.resume(Response.serverError().build());
    }

}

