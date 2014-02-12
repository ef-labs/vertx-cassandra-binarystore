package com.englishtown.vertx.cassandra.binarystore;

import java.util.UUID;

/**
 * Created by adriangonzalez on 2/12/14.
 */
public interface ChunkInfo {

    UUID getId();

    int getNum();

    byte[] getData();

}
