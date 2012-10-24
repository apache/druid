package com.metamx.druid.index.v1.processing;

import com.metamx.druid.QueryGranularity;
import com.metamx.druid.index.brita.Filter;
import org.joda.time.Interval;

/**
 */
public interface CursorFactory
{
  public Iterable<Cursor> makeCursors(Filter filter, Interval interval, QueryGranularity gran);
}
