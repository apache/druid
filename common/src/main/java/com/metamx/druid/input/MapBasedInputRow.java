package com.metamx.druid.input;

import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

/**
 */
public class MapBasedInputRow extends MapBasedRow implements InputRow
{
  private final List<String> dimensions;

  public MapBasedInputRow(
      long timestamp,
      List<String> dimensions,
      Map<String, Object> event
  )
  {
    super(timestamp, event);
    this.dimensions = dimensions;
  }

  @Override
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @Override
  public String toString()
  {
    return "MapBasedInputRow{" +
           "timestamp=" + new DateTime(getTimestampFromEpoch()) +
           ", event=" + getEvent() +
           ", dimensions=" + dimensions +
           '}';
  }
}
