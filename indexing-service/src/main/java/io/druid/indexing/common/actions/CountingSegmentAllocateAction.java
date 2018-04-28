/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.druid.indexing.common.Counters;
import io.druid.indexing.common.task.Task;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Intervals;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.SortedSet;

/**
 * This action is to find a proper {@link io.druid.timeline.partition.ShardSpec} based on counting. This keeps track of
 * the next shard number per {@link Interval} in {@link Counters}. The next shard number is incremented by 1 whenever a
 * new {@link SegmentIdentifier} is allocated.
 */
public class CountingSegmentAllocateAction implements TaskAction<SegmentIdentifier>
{
  private final String dataSource;
  private final DateTime timestamp;
  private final GranularitySpec granularitySpec;
  @JsonDeserialize(keyUsing = IntervalDeserializer.class)
  private final Map<Interval, String> versions;

  private final SortedSet<Interval> bucketIntervals;

  @JsonCreator
  public CountingSegmentAllocateAction(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("timestamp") DateTime timestamp,
      @JsonProperty("granularitySpec") GranularitySpec granularitySpec,
      @JsonProperty("versions") Map<Interval, String> versions
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.timestamp = Preconditions.checkNotNull(timestamp, "timestamp");
    this.granularitySpec = Preconditions.checkNotNull(granularitySpec, "granularitySpec");
    this.versions = Preconditions.checkNotNull(versions, "versions");

    this.bucketIntervals = Preconditions.checkNotNull(granularitySpec.bucketIntervals().orNull(), "bucketIntervals");
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public DateTime getTimestamp()
  {
    return timestamp;
  }

  @JsonProperty
  public GranularitySpec getGranularitySpec()
  {
    return granularitySpec;
  }

  @JsonProperty
  public Map<Interval, String> getVersions()
  {
    return versions;
  }

  @Override
  public TypeReference<SegmentIdentifier> getReturnTypeReference()
  {
    return new TypeReference<SegmentIdentifier>()
    {
    };
  }

  @Override
  public SegmentIdentifier perform(Task task, TaskActionToolbox toolbox)
  {
    Optional<Interval> maybeInterval = granularitySpec.bucketInterval(timestamp);
    if (!maybeInterval.isPresent()) {
      throw new ISE("Could not find interval for timestamp [%s]", timestamp);
    }

    final Interval interval = maybeInterval.get();
    if (!bucketIntervals.contains(interval)) {
      throw new ISE("Unspecified interval[%s] in granularitySpec[%s]", interval, granularitySpec);
    }

    final Counters counters = toolbox.getCounters();

    final int partitionNum = counters.increment(interval.toString(), 1);
    return new SegmentIdentifier(
        dataSource,
        interval,
        findVersion(versions, interval),
        new NumberedShardSpec(partitionNum, 0)
    );
  }

  @Override
  public boolean isAudited()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "CountingSegmentAllocateAction{" +
           "dataSource='" + dataSource + '\'' +
           ", timestamp=" + timestamp +
           ", granularitySpec=" + granularitySpec +
           ", versions=" + versions +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final CountingSegmentAllocateAction that = (CountingSegmentAllocateAction) o;
    if (!dataSource.equals(that.dataSource)) {
      return false;
    }
    if (!timestamp.equals(that.timestamp)) {
      return false;
    }
    if (!granularitySpec.equals(that.granularitySpec)) {
      return false;
    }
    return versions.equals(that.versions);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, timestamp, granularitySpec, versions);
  }

  private static String findVersion(Map<Interval, String> versions, Interval interval)
  {
    return versions.entrySet().stream()
                   .filter(entry -> entry.getKey().contains(interval))
                   .map(Entry::getValue)
                   .findFirst()
                   .orElseThrow(() -> new ISE("Cannot find a version for interval[%s]", interval));
  }

  public static class IntervalDeserializer extends KeyDeserializer
  {
    @Override
    public Object deserializeKey(String s, DeserializationContext deserializationContext)
    {
      return Intervals.of(s);
    }
  }
}
