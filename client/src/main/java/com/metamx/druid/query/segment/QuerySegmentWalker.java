package com.metamx.druid.query.segment;

import com.metamx.druid.Query;
import com.metamx.druid.query.QueryRunner;
import org.joda.time.Interval;

/**
 */
public interface QuerySegmentWalker
{
  /**
   * Gets the Queryable for a given interval, the Queryable returned can be any version(s) or partitionNumber(s)
   * such that it represents the interval.
   *
   * @param intervals the intervals to find a Queryable for
   * @return a Queryable object that represents the interval
   */
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals);

  /**
   * Gets the Queryable for a given list of SegmentSpecs.
   * exist.
   *
   * @return the Queryable object with the given SegmentSpecs
   */
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs);
}
