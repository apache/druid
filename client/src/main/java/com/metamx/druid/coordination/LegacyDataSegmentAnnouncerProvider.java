package com.metamx.druid.coordination;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.druid.guice.annotations.Json;

import javax.validation.constraints.NotNull;
import java.util.Arrays;

/**
 */
public class LegacyDataSegmentAnnouncerProvider implements DataSegmentAnnouncerProvider
{
  @JacksonInject
  @NotNull
  private SingleDataSegmentAnnouncer singleAnnouncer = null;

  @JacksonInject
  @NotNull
  private BatchDataSegmentAnnouncer batchAnnouncer = null;

  @Override
  public DataSegmentAnnouncer get()
  {
    return new MultipleDataSegmentAnnouncerDataSegmentAnnouncer(
        Arrays.<DataSegmentAnnouncer>asList(singleAnnouncer, batchAnnouncer)
    );
  }
}
