package com.englishtown.vertx.cassandra.binarystore;

import java.util.UUID;

/**
 * Created by adriangonzalez on 3/10/14.
 */
public interface BinaryStoreReader {

    /**
     * Reads a binary file from the store and writes to the provided write stream
     *
     * @param id
     * @return
     */
    FileReader read(UUID id);

    /**
     * Reads a binary file range from the store and writes to the provided write stream
     *
     * @param id
     * @param range
     * @return
     */
    FileReader readRange(UUID id, ContentRange range);

}
