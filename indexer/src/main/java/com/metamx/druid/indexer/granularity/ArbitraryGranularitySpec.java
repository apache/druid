package com.metamx.druid.indexer.granularity;

import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.metamx.common.guava.Comparators;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class ArbitraryGranularitySpec implements GranularitySpec
{
  private final TreeSet<Interval> intervals;

  @JsonCreator
  public ArbitraryGranularitySpec(
      @JsonProperty("intervals") List<Interval> inputIntervals
  )
  {
    intervals = Sets.newTreeSet(Comparators.intervalsByStartThenEnd());

    // Insert all intervals
    for(final Interval inputInterval : inputIntervals) {
      intervals.add(inputInterval);
    }

    // Ensure intervals are non-overlapping (but they may abut each other)
    final PeekingIterator<Interval> intervalIterator = Iterators.peekingIterator(intervals.iterator());
    while(intervalIterator.hasNext()) {
      final Interval currentInterval = intervalIterator.next();

      if(intervalIterator.hasNext()) {
        final Interval nextInterval = intervalIterator.peek();
        if(currentInterval.overlaps(nextInterval)) {
          throw new IllegalArgumentException(String.format(
              "Overlapping intervals: %s, %s",
              currentInterval,
              nextInterval));
        }
      }
    }
  }

  @Override
  @JsonProperty("intervals")
  public SortedSet<Interval> bucketIntervals()
  {
    return intervals;
  }

  @Override
  public Optional<Interval> bucketInterval(DateTime dt)
  {
    // First interval with start time ≤ dt
    final Interval interval = intervals.floor(new Interval(dt, new DateTime(Long.MAX_VALUE)));

    if(interval != null && interval.contains(dt)) {
      return Optional.of(interval);
    } else {
      return Optional.absent();
    }
  }
}
