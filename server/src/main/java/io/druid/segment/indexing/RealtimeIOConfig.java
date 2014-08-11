package io.druid.segment.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.input.FirehoseFactory;
import io.druid.segment.realtime.plumber.PlumberSchool;

/**
 */
public class RealtimeIOConfig implements IOConfig
{
  private final FirehoseFactory firehoseFactory;
  private final PlumberSchool plumberSchool;

  @JsonCreator
  public RealtimeIOConfig(
      @JsonProperty("firehose") FirehoseFactory firehoseFactory,
      @JsonProperty("plumber") PlumberSchool plumberSchool
  )
  {
    this.firehoseFactory = firehoseFactory;
    this.plumberSchool = plumberSchool;
  }

  @JsonProperty("firehose")
  public FirehoseFactory getFirehoseFactory()
  {
    return firehoseFactory;
  }

  public PlumberSchool getPlumberSchool()
  {
    return plumberSchool;
  }
}
