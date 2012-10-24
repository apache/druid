package com.metamx.druid.jackson;

import com.google.common.base.Joiner;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.ser.std.ScalarSerializerBase;

import java.io.IOException;
import java.util.List;

/**
 */
public class CommaListJoinSerializer extends ScalarSerializerBase<List<String>>
{
  private static final Joiner joiner = Joiner.on(",");

  protected CommaListJoinSerializer()
  {
    super(List.class, true);
  }

  @Override
  public void serialize(List<String> value, JsonGenerator jgen, SerializerProvider provider)
      throws IOException, JsonGenerationException
  {
    jgen.writeString(joiner.join(value));
  }
}
