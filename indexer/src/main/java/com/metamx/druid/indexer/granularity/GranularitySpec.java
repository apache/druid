package com.metamx.druid.indexer.granularity;

import com.google.common.base.Optional;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.SortedSet;

/**
 * Tells the indexer how to group events based on timestamp. The events may then be further partitioned based
 *  on anything, using a ShardSpec.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = UniformGranularitySpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "uniform", value = UniformGranularitySpec.class),
    @JsonSubTypes.Type(name = "arbitrary", value = ArbitraryGranularitySpec.class)
})
public interface GranularitySpec
{
  /** Set of all time groups, broken up on segment boundaries. Should be sorted by interval start and non-overlapping.*/
  public SortedSet<Interval> bucketIntervals();

  /** Time-grouping interval corresponding to some instant, if any. */
  public Optional<Interval> bucketInterval(DateTime dt);
}
