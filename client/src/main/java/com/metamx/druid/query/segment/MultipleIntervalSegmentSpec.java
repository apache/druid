package com.metamx.druid.query.segment;

import com.metamx.druid.Query;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.utils.JodaUtils;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.List;

/**
 */
public class MultipleIntervalSegmentSpec implements QuerySegmentSpec
{
  private final List<Interval> intervals;

  @JsonCreator
  public MultipleIntervalSegmentSpec(
      @JsonProperty("intervals") List<Interval> intervals
  )
  {
    this.intervals = Collections.unmodifiableList(JodaUtils.condenseIntervals(intervals));
  }

  @Override
  @JsonProperty("intervals")
  public List<Interval> getIntervals()
  {
    return intervals;
  }

  @Override
  public <T> QueryRunner<T> lookup(Query<T> query, QuerySegmentWalker walker)
  {
    return walker.getQueryRunnerForIntervals(query, intervals);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "intervals=" + intervals +
           '}';
  }
}
