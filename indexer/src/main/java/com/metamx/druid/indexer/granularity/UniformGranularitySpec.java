package com.metamx.druid.indexer.granularity;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.metamx.common.Granularity;
import com.metamx.common.guava.Comparators;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class UniformGranularitySpec implements GranularitySpec
{
  final private Granularity granularity;
  final private List<Interval> intervals;

  @JsonCreator
  public UniformGranularitySpec(
      @JsonProperty("gran") Granularity granularity,
      @JsonProperty("intervals") List<Interval> intervals
  )
  {
    this.granularity = granularity;
    this.intervals = intervals;
  }

  @Override
  public SortedSet<Interval> bucketIntervals()
  {
    final TreeSet<Interval> retVal = Sets.newTreeSet(Comparators.intervals());

    for (Interval interval : intervals) {
      for (Interval segmentInterval : granularity.getIterable(interval)) {
        retVal.add(segmentInterval);
      }
    }

    return retVal;
  }

  @Override
  public Optional<Interval> bucketInterval(DateTime dt)
  {
    return Optional.of(granularity.bucket(dt));
  }

  @JsonProperty
  public Granularity getGranularity()
  {
    return granularity;
  }

  @JsonProperty
  public Iterable<Interval> getIntervals()
  {
    return intervals;
  }
}
