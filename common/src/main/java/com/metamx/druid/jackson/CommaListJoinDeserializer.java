package com.metamx.druid.jackson;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.deser.std.StdScalarDeserializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 */
public class CommaListJoinDeserializer extends StdScalarDeserializer<List<String>>
{
    protected CommaListJoinDeserializer()
  {
    super(List.class);
  }

  @Override
  public List<String> deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException, JsonProcessingException
  {
    return Arrays.asList(jsonParser.getText().split(","));
  }
}
