package com.metamx.druid.indexer.data;

<<<<<<< HEAD
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
=======
import com.metamx.common.exception.FormattedException;
>>>>>>> 68a3f1ab79feb1677e4174728e5209208a54bad9
import com.metamx.druid.input.InputRow;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = StringInputRowParser.class)
@JsonSubTypes({
        @JsonSubTypes.Type(name = "string", value = StringInputRowParser.class),
        @JsonSubTypes.Type(name = "protobuf", value = ProtoBufInputRowParser.class)
})
public interface InputRowParser<T>
{
  public InputRow parse(T input) throws FormattedException;
  public void addDimensionExclusion(String dimension);
}
