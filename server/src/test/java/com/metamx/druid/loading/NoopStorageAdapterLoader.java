package com.metamx.druid.loading;

import com.metamx.druid.Capabilities;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.StorageAdapter;
import com.metamx.druid.index.brita.Filter;
import com.metamx.druid.index.v1.processing.Cursor;
import com.metamx.druid.query.search.SearchHit;
import com.metamx.druid.query.search.SearchQuery;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Map;

/**
*/
public class NoopStorageAdapterLoader implements StorageAdapterLoader
{
  @Override
  public StorageAdapter getAdapter(final Map<String, Object> loadSpec)
  {
    return new StorageAdapter()
    {
      @Override
      public String getSegmentIdentifier()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public Interval getInterval()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getDimensionCardinality(String dimension)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public DateTime getMinTime()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public DateTime getMaxTime()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public Capabilities getCapabilities()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public Iterable<Cursor> makeCursors(Filter filter, Interval interval, QueryGranularity gran)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public Iterable<SearchHit> searchDimensions(SearchQuery query, Filter filter)
      {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public void cleanupAdapter(Map<String, Object> loadSpec) throws StorageAdapterLoadingException
  {

  }
}
