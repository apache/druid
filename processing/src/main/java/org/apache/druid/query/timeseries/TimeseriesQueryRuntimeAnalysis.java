package org.apache.druid.query.timeseries;

import org.apache.druid.query.QueryRuntimeAnalysis;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryMetrics;

public class TimeseriesQueryRuntimeAnalysis extends QueryRuntimeAnalysis<TimeseriesQuery, TimeseriesQueryMetrics>
    implements TimeseriesQueryMetrics
{
  public TimeseriesQueryRuntimeAnalysis(TimeseriesQueryMetrics delegate)
  {
    super(delegate);
  }

  @Override
  public void limit(TimeseriesQuery query)
  {

  }

  @Override
  public void numMetrics(TimeseriesQuery query)
  {

  }

  @Override
  public void numComplexMetrics(TimeseriesQuery query)
  {

  }

  @Override
  public void granularity(TimeseriesQuery query)
  {

  }
}
