package com.metamx.druid.indexer.data;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.nio.ByteBuffer;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = StringInputRowParser.class)
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = "protobuf", value = ProtoBufInputRowParser.class),
        @JsonSubTypes.Type(name = "string", value = StringInputRowParser.class)
})
public interface ByteBufferInputRowParser extends InputRowParser<ByteBuffer> {
}
