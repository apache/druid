package com.metamx.druid.coordination;

import com.fasterxml.jackson.annotation.JacksonInject;

import javax.validation.constraints.NotNull;

/**
 */
public class BatchDataSegmentAnnouncerProvider implements DataSegmentAnnouncerProvider
{
  @JacksonInject
  @NotNull
  private BatchDataSegmentAnnouncer batchAnnouncer = null;

  @Override
  public DataSegmentAnnouncer get()
  {
    return batchAnnouncer;
  }
}
