package com.metamx.druid.query.segment;

import com.metamx.druid.Query;
import com.metamx.druid.query.QueryRunner;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;

/**
*/
public class SpecificSegmentSpec implements QuerySegmentSpec
{
  private final SegmentDescriptor descriptor;

  public SpecificSegmentSpec(
      SegmentDescriptor descriptor
  ) {
    this.descriptor = descriptor;
  }

  @Override
  public List<Interval> getIntervals()
  {
    return Arrays.asList(descriptor.getInterval());
  }

  @Override
  public <T> QueryRunner<T> lookup(Query<T> query, QuerySegmentWalker walker)
  {
    return walker.getQueryRunnerForSegments(query, Arrays.asList(descriptor));
  }
}
