package com.metamx.druid.index.v1;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.metamx.druid.kv.Indexed;
import com.metamx.druid.kv.IndexedInts;
import org.joda.time.Interval;

/**
 */
public class RowboatFilteringIndexAdapter implements IndexableAdapter
{
  private final IndexableAdapter baseAdapter;
  private final Predicate<Rowboat> filter;

  public RowboatFilteringIndexAdapter(IndexableAdapter baseAdapter, Predicate<Rowboat> filter)
  {
    this.baseAdapter = baseAdapter;
    this.filter = filter;
  }

  @Override
  public Interval getDataInterval()
  {
    return baseAdapter.getDataInterval();
  }

  @Override
  public int getNumRows()
  {
    return baseAdapter.getNumRows();
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return baseAdapter.getAvailableDimensions();
  }

  @Override
  public Indexed<String> getAvailableMetrics()
  {
    return baseAdapter.getAvailableMetrics();
  }

  @Override
  public Indexed<String> getDimValueLookup(String dimension)
  {
    return baseAdapter.getDimValueLookup(dimension);
  }

  @Override
  public Iterable<Rowboat> getRows()
  {
    return Iterables.filter(baseAdapter.getRows(), filter);
  }

  @Override
  public IndexedInts getInverteds(String dimension, String value)
  {
    return baseAdapter.getInverteds(dimension, value);
  }

  @Override
  public String getMetricType(String metric)
  {
    return baseAdapter.getMetricType(metric);
  }
}
