package com.metamx.druid.indexer.data;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.metamx.druid.input.InputRow;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = StringInputRowParser.class)
@JsonSubTypes({
        @JsonSubTypes.Type(name = "string", value = StringInputRowParser.class),
        @JsonSubTypes.Type(name = "protobuf", value = ProtoBufInputRowParser.class),
})
public interface InputRowParser<T>
{
  public InputRow parse(T input);
  public void addDimensionExclusion(String dimension);
}
