package com.metamx.druid.query.segment;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.metamx.druid.Query;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.utils.JodaUtils;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.Interval;

import java.util.List;

/**
 */
public class MultipleSpecificSegmentSpec implements QuerySegmentSpec
{
  private final List<SegmentDescriptor> descriptors;

  private volatile List<Interval> intervals = null;

  @JsonCreator
  public MultipleSpecificSegmentSpec(
      @JsonProperty("segments") List<SegmentDescriptor> descriptors
  )
  {
    this.descriptors = descriptors;
  }

  @JsonProperty("segments")
  public List<SegmentDescriptor> getDescriptors()
  {
    return descriptors;
  }

  @Override
  public List<Interval> getIntervals()
  {
    if (intervals != null) {
      return intervals;
    }

    intervals = JodaUtils.condenseIntervals(
        Iterables.transform(
            descriptors,
            new Function<SegmentDescriptor, Interval>()
            {
              @Override
              public Interval apply(SegmentDescriptor input)
              {
                return input.getInterval();
              }
            }
        )
    );

    return intervals;
  }

  @Override
  public <T> QueryRunner<T> lookup(Query<T> query, QuerySegmentWalker walker)
  {
    return walker.getQueryRunnerForSegments(query, descriptors);
  }

  @Override
  public String toString()
  {
    return "MultipleSpecificSegmentSpec{" +
           "descriptors=" + descriptors +
           '}';
  }
}
