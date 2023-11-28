package org.apache.druid.server.coordination;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.segment.realtime.appenderator.SinksSchema;

import javax.annotation.Nullable;

/**
 *
 */
public class SinkSchemaChangeRequest implements DataSegmentChangeRequest
{
  private final SinksSchema sinksSchema;

  @JsonCreator
  public SinkSchemaChangeRequest(
      @JsonProperty("sinksSchemaChange") SinksSchema sinksSchema
  )
  {
    this.sinksSchema = sinksSchema;
  }

  @JsonProperty
  public SinksSchema getSinksSchemaChange()
  {
    return sinksSchema;
  }

  @Override
  public void go(DataSegmentChangeHandler handler, @Nullable DataSegmentChangeCallback callback)
  {
  }

  @Override
  public String asString()
  {
    return null;
  }
}
