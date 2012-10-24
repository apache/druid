package com.metamx.druid.index.v1;

import com.metamx.druid.Capabilities;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.StorageAdapter;
import com.metamx.druid.index.brita.Filter;
import com.metamx.druid.index.v1.processing.Cursor;
import com.metamx.druid.query.search.SearchHit;
import com.metamx.druid.query.search.SearchQuery;
import org.joda.time.DateTime;
import org.joda.time.Interval;

/**
 */
public class SegmentIdAttachedStorageAdapter implements StorageAdapter
{
  private final String segmentId;
  private final StorageAdapter delegate;

  public SegmentIdAttachedStorageAdapter(
      String segmentId,
      StorageAdapter delegate
  )
  {
    this.segmentId = segmentId;
    this.delegate = delegate;
  }

  @Override
  public String getSegmentIdentifier()
  {
    return segmentId;
  }

  @Override
  public Interval getInterval()
  {
    return delegate.getInterval();
  }

  @Override
  public Iterable<SearchHit> searchDimensions(SearchQuery query, Filter filter)
  {
    return delegate.searchDimensions(query, filter);
  }

  @Override
  public Iterable<Cursor> makeCursors(Filter filter, Interval interval, QueryGranularity gran)
  {
    return delegate.makeCursors(filter, interval, gran);
  }

  @Override
  public Capabilities getCapabilities()
  {
    return delegate.getCapabilities();
  }

  @Override
  public DateTime getMaxTime()
  {
    return delegate.getMaxTime();
  }

  @Override
  public DateTime getMinTime()
  {
    return delegate.getMinTime();
  }

  @Override
  public int getDimensionCardinality(String dimension)
  {
    return delegate.getDimensionCardinality(dimension);
  }

  public StorageAdapter getDelegate()
  {
    return delegate;
  }
}
