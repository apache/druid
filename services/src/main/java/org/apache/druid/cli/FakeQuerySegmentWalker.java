package org.apache.druid.cli;

import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.SegmentDescriptor;
import org.joda.time.Interval;

public class FakeQuerySegmentWalker implements QuerySegmentWalker
{
  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    return null;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    return null;
  }
}