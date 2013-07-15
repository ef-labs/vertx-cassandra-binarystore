# Cassandra Binary Store

Allows storing large binary files in Cassandra via the Vert.x event bus keeping file chunks in a binary format.  The files are kept in two Cassandra tables:
1. files - contains file information such as filename, contentType, length, chunkSize, and additional metadata
2. chunks - contains the chunks of binary data

[![Build Status](https://travis-ci.org/englishtown/vertx-mod-cassandra-binarystore.png)](https://travis-ci.org/englishtown/vertx-mod-cassandra-binarystore)

## Dependencies

This module requires a Cassandra 1.2+ server to be available on the network.


## Configuration

The Cassandra Binary Store module takes the following configuration:

    {
        "address": <address>,
        "ips": [<ips>],
        "keyspace": <keyspace>
    }

For example:

    {
        "address": "test.my_persistor",
        "ips": ["192.168.1.100"],
        "keyspace": "my_keyspace"
    }

Let's take a look at each field in turn:

* `address` The main address for the module. Every module has a main address. Defaults to `et.cassandra.binarystore`.
* `ips` An array of ip addresses for the Cassandra cluster. Defaults to `127.0.0.1`.
* `keyspace` The keyspace name in Cassandra instance to use. Defaults to `binarystore`.


## Operations

The module supports the following operations

### Get File

Returns file information for a given id.

Send a JSON message to the module main address:

    {
        "action": "getFile",
        "id": <id>
    }

Where:
* `id` is the UUID of the file. This field is mandatory.

An example would be:

    {
        "action": "getFile",
        "id": "51d864754728011036adc575"
    }

When the getFile completes successfully, a reply message is sent back to the sender with the following data:

    {
        "status": "ok",
        "filename": <filename>,
        "contentType": <contentType>,
        "length": <length>,
        "chunkSize": <chunkSize>,
        "uploadDate": <uploadDate>,
        "metadata": <metadata>
    }

Where:
* `filename` is the filename provided when saving
* `contentType` is the content type (ex. image/jpeg)
* `length` is the total file length in bytes
* `chunkSize` is the size in bytes of each chunk
* `uploadDate` is the long time of the upload in milliseconds since 1 Jan 1970
* `metadata` is an optional json object with additional metadata

If an error occurs in saving the document a reply is returned:

    {
        "status": "error",
        "message": <message>
    }

Where `message` is an error message.


### Get Chunk

Returns a file chunk

Send a JSON message to the module main address:

    {
        "action": "getChunk",
        "files_id": <files_id>,
        "n": <n>,
        "reply": <reply>
    }

Where:
* `files_id` is the UUID of the file
* `n` is the chunk number (first chunk is 0).
* `reply` is a boolean flag indicating a reply message handler should be added to send the next chunk


An example would be:

    {
        "action": "getChunk",
        "files_id": "51d864754728011036adc575",
        "n": 0,
        "reply": true
    }

When the get chunk completes successfully, a reply message with the chunk data byte[] in the message body is returned.

If an error occurs when getting the chunk, a json message is returned:

    {
        "status": "error",
        "message": <message>
    }

Where `message` is an error message.


### Save File

Saves the file information.

Send a JSON message to the module main address:

    {
        "action": "saveFile",
        "id": <id>,
        "length": <length>,
        "chunkSize": <chunkSize>,
        "uploadDate": <uploadDate>,
        "filename": <filename>,
        "contentType": <contentType>
    }

Where:
* `id` is the UUID of the file.
* `length` is the total file length in bytes
* `chunkSize` is the size in bytes of each chunk
* `uploadDate` is the long time of the upload in milliseconds since 1 Jan 1970.  The field is optional.
* `filename` is the filename provided when saving.  This field is optional.
* `contentType` is the content type (ex. image/jpeg).  This field is optional (but recommended).

An example would be:

    {
        "action": "saveFile",
        "id": "51d864754728011036adc575",
        "length": 161966,
        "chunkSize": 102400,
        "contentType": "image/jpeg"
    }

When the save completes successfully, a reply message is sent back to the sender with the following data:

    {
        "status": "ok"
    }

If an error occurs when saving the file information a reply is returned:

    {
        "status": "error",
        "message": <message>
    }

Where `message` is an error message.


### Save Chunk

Saves a chunk of binary data.

Send a Buffer message to the module main address + "/saveChunk".

The Buffer is made up of 3 parts:
1. The first four bytes are an int defining the length of a UTF-8 encoded json string.
2. The json bytes are next
3. The remaining bytes are the chunk to be saved

The json contains the following fields:

    {
        "files_id": <files_id>,
        "n": <n>
    }

Where:
* `files_id` is the UUID of the file
* `n` is the chunk number (first chunk is 0).


An example would be:

    {
        "files_id": "51d864754728011036adc575",
        "n": 0
    }


An example of generating the message would be:

```java
public Buffer getMessage(String files_id, int chunkNumber, byte[] data) {
    JsonObject jsonObject = new JsonObject()
            .putString("files_id", files_id)
            .putNumber("n", chunkNumber);
    byte[] json = jsonObject.encode().getBytes("UTF-8");
    byte[] data = getData(chunkNumber);

    Buffer buffer = new Buffer();
    buffer.appendInt(json.length);
    buffer.appendBytes(json);
    buffer.appendBytes(data);

    return buffer;
}
```


When the save completes successfully, a reply message is sent back to the sender with the following data:

    {
         "status": "ok"
    }
