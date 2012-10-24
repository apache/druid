package com.metamx.druid.index.v1.processing;

import com.metamx.druid.processing.MetricSelectorFactory;
import org.joda.time.DateTime;

/**
 */
public interface Cursor extends MetricSelectorFactory, DimensionSelectorFactory
{
  public DateTime getTime();
  public void advance();
  public boolean isDone();
  public void reset();
}
