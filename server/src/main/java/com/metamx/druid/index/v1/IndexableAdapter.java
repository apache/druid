package com.metamx.druid.index.v1;

import com.metamx.druid.kv.Indexed;
import com.metamx.druid.kv.IndexedInts;
import org.joda.time.Interval;

/**
 */
public interface IndexableAdapter
{
  Interval getDataInterval();

  int getNumRows();

  Indexed<String> getAvailableDimensions();

  Indexed<String> getAvailableMetrics();

  Indexed<String> getDimValueLookup(String dimension);

  Iterable<Rowboat> getRows();

  IndexedInts getInverteds(String dimension, String value);

  String getMetricType(String metric);
}
