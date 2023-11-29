package org.apache.druid.query.groupby;

import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryRuntimeAnalysis;

public class GroupByQueryRuntimeAnalysis extends QueryRuntimeAnalysis<GroupByQuery, GroupByQueryMetrics> implements GroupByQueryMetrics
{
  public GroupByQueryRuntimeAnalysis(GroupByQueryMetrics delegate)
  {
    super(delegate);
  }

  @Override
  public void numDimensions(GroupByQuery query)
  {
    delegate.numDimensions(query);
  }

  @Override
  public void numMetrics(GroupByQuery query)
  {
    delegate.numMetrics(query);
  }

  @Override
  public void numComplexMetrics(GroupByQuery query)
  {
    delegate.numComplexMetrics(query);
  }

  @Override
  public void granularity(GroupByQuery query)
  {
    delegate.granularity(query);
  }
}
