package com.metamx.druid.index.v1;

import com.metamx.druid.aggregation.Aggregator;

import java.io.IOException;

/**
 */
public interface MetricColumnSerializer
{
  public void open() throws IOException;
  public void serialize(Object aggs) throws IOException;
  public void close() throws IOException;
}
