package com.englishtown.vertx.cassandra.binarystore;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Binary store file info
 */
@DataObject
public class FileInfo {

    UUID id;
    String fileName;
    String contentType;
    long length;
    int chunkSize;
    long uploadDate;
    Map<String, String> metadata;

    private static final String JSON_FIELD_ID = "id";
    private static final String JSON_FIELD_FILENAME = "filename";
    private static final String JSON_FIELD_CONTENT_TYPE = "contentType";
    private static final String JSON_FIELD_LENGTH = "length";
    private static final String JSON_FIELD_CHUNK_SIZE = "chunkSize";
    private static final String JSON_FIELD_UPLOAD_DATE = "uploadDate";
    private static final String JSON_FIELD_METADATA = "metadata";

    public FileInfo() {
    }

    public FileInfo(FileInfo other) {
        id = other.getId();
        fileName = other.getFileName();
        contentType = other.getContentType();
        length = other.getLength();
        chunkSize = other.getChunkSize();
        uploadDate = other.getUploadDate();
        metadata = other.getMetadata();
    }

    public FileInfo(JsonObject json) {

        String s = json.getString(JSON_FIELD_ID);
        if (s != null) {
            id = UUID.fromString(s);
        }

        length = json.getLong(JSON_FIELD_LENGTH, 0L);
        chunkSize = json.getInteger(JSON_FIELD_CHUNK_SIZE, 0);
        uploadDate = json.getLong(JSON_FIELD_UPLOAD_DATE, 0L);

        String filename = json.getString(JSON_FIELD_FILENAME);
        String contentType = json.getString(JSON_FIELD_CONTENT_TYPE);
        JsonObject metadata = json.getJsonObject(JSON_FIELD_METADATA);

        FileInfo fileInfo = new FileInfo()
                .setId(id)
                .setLength(length)
                .setChunkSize(chunkSize)
                .setUploadDate(uploadDate)
                .setFileName(filename)
                .setContentType(contentType);

        if (metadata != null) {
            Map<String, String> map = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : metadata) {
                map.put(entry.getKey(), (entry.getValue() == null ? null : String.valueOf(entry.getValue())));
            }
            fileInfo.setMetadata(map);
        }

    }

    public UUID getId() {
        return id;
    }

    public FileInfo setId(UUID id) {
        this.id = id;
        return this;
    }

    public String getFileName() {
        return fileName;
    }

    public FileInfo setFileName(String fileName) {
        this.fileName = fileName;
        return this;
    }

    public String getContentType() {
        return contentType;
    }

    public FileInfo setContentType(String contentType) {
        this.contentType = contentType;
        return this;
    }

    public long getLength() {
        return length;
    }

    public FileInfo setLength(long length) {
        this.length = length;
        return this;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public int getChunkCount() {
        if (getChunkSize() <= 0) {
            return 0;
        }
        double count = (double) getLength() / getChunkSize();
        return (int) Math.ceil(count);
    }

    public FileInfo setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    public long getUploadDate() {
        return uploadDate;
    }

    public FileInfo setUploadDate(long uploadDate) {
        this.uploadDate = uploadDate;
        return this;
    }

    public Map<String, String> getMetadata() {
        return this.metadata;
    }

    public FileInfo setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
        return this;
    }

    public JsonObject toJson() {

        JsonObject json = new JsonObject()
                .put(JSON_FIELD_FILENAME, getFileName())
                .put(JSON_FIELD_CONTENT_TYPE, getContentType())
                .put(JSON_FIELD_LENGTH, getLength())
                .put(JSON_FIELD_CHUNK_SIZE, getChunkSize())
                .put(JSON_FIELD_UPLOAD_DATE, getUploadDate());

        if (getId() != null) {
            json.put(JSON_FIELD_ID, getId().toString());
        }
        if (getMetadata() != null) {
            json.put(JSON_FIELD_METADATA, getMetadata());
        }
        if (getMetadata() != null) {
            JsonObject metadata = new JsonObject();
            for (Map.Entry<String, String> entry : getMetadata().entrySet()) {
                metadata.put(entry.getKey(), entry.getValue());
            }
            json.put("metadata", metadata);
        }

        return json;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (obj == this) return true;

        if (!(obj instanceof FileInfo)) return false;

        FileInfo other = (FileInfo) obj;

        if (!Objects.equals(this.getId(), other.getId())) return false;
        if (!Objects.equals(this.getFileName(), other.getFileName())) return false;
        if (!Objects.equals(this.getContentType(), other.getContentType())) return false;
        if (!Objects.equals(this.getLength(), other.getLength())) return false;
        if (!Objects.equals(this.getChunkSize(), other.getChunkSize())) return false;
        if (!Objects.equals(this.getUploadDate(), other.getUploadDate())) return false;

        return true;
    }
}
