package com.metamx.druid;

import com.metamx.druid.index.v1.Searchable;
import com.metamx.druid.index.v1.processing.CursorFactory;
import org.joda.time.DateTime;
import org.joda.time.Interval;

/**
 */
public interface StorageAdapter extends CursorFactory, Searchable
{
  public String getSegmentIdentifier();
  public Interval getInterval();
  public int getDimensionCardinality(String dimension);
  public DateTime getMinTime();
  public DateTime getMaxTime();
  public Capabilities getCapabilities();
}
