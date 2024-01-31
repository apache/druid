/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.msq.indexing.destination;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.msq.querykit.ShuffleSpecFactories;
import org.apache.druid.msq.querykit.ShuffleSpecFactory;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceType;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class DataSourceMSQDestination implements MSQDestination
{
  public static final String TYPE = "dataSource";

  private final String dataSource;
  private final Granularity segmentGranularity;
  private final List<String> segmentSortOrder;

  @Nullable
  private final List<Interval> replaceTimeChunks;

  @JsonCreator
  public DataSourceMSQDestination(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("segmentGranularity") Granularity segmentGranularity,
      @JsonProperty("segmentSortOrder") @Nullable List<String> segmentSortOrder,
      @JsonProperty("replaceTimeChunks") @Nullable List<Interval> replaceTimeChunks
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.segmentGranularity = Preconditions.checkNotNull(segmentGranularity, "segmentGranularity");
    this.segmentSortOrder = segmentSortOrder != null ? segmentSortOrder : Collections.emptyList();
    this.replaceTimeChunks = replaceTimeChunks;

    if (replaceTimeChunks != null) {
      // Verify that if replaceTimeChunks is provided, it is nonempty.
      if (replaceTimeChunks.isEmpty()) {
        throw new IAE("replaceTimeChunks must be null or nonempty; cannot be empty");
      }

      // Verify all provided time chunks are aligned with segmentGranularity.
      for (final Interval interval : replaceTimeChunks) {
        // ETERNITY gets a free pass.
        if (!Intervals.ETERNITY.equals(interval)) {
          final boolean startIsAligned =
              segmentGranularity.bucketStart(interval.getStart()).equals(interval.getStart());

          final boolean endIsAligned =
              segmentGranularity.bucketStart(interval.getEnd()).equals(interval.getEnd())
              || segmentGranularity.increment(segmentGranularity.bucketStart(interval.getEnd()))
                                   .equals(interval.getEnd());

          if (!startIsAligned || !endIsAligned) {
            throw new IAE(
                "Time chunk [%s] provided in replaceTimeChunks is not aligned with segmentGranularity [%s]",
                interval,
                segmentGranularity
            );
          }
        }
      }
    }
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public Granularity getSegmentGranularity()
  {
    return segmentGranularity;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<String> getSegmentSortOrder()
  {
    return segmentSortOrder;
  }

  /**
   * Returns the list of time chunks to replace, or null if {@link #isReplaceTimeChunks()} is false.
   *
   * Invariants: if nonnull, then this will *also* be nonempty, and all intervals will be aligned
   * with {@link #getSegmentGranularity()}. Each interval may comprise multiple time chunks.
   */
  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<Interval> getReplaceTimeChunks()
  {
    return replaceTimeChunks;
  }

  /**
   * Whether this object is in replace-existing-time-chunks mode.
   */
  public boolean isReplaceTimeChunks()
  {
    return replaceTimeChunks != null;
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
    DataSourceMSQDestination that = (DataSourceMSQDestination) o;
    return Objects.equals(dataSource, that.dataSource)
           && Objects.equals(segmentGranularity, that.segmentGranularity)
           && Objects.equals(segmentSortOrder, that.segmentSortOrder)
           && Objects.equals(replaceTimeChunks, that.replaceTimeChunks);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, segmentGranularity, segmentSortOrder, replaceTimeChunks);
  }

  @Override
  public String toString()
  {
    return "DataSourceMSQDestination{" +
           "dataSource='" + dataSource + '\'' +
           ", segmentGranularity=" + segmentGranularity +
           ", segmentSortOrder=" + segmentSortOrder +
           ", replaceTimeChunks=" + replaceTimeChunks +
           '}';
  }

  @Override
  public ShuffleSpecFactory getShuffleSpecFactory(int targetSize)
  {
    return ShuffleSpecFactories.getGlobalSortWithTargetSize(targetSize);
  }

  @Override
  public Optional<Resource> getDestinationResource()
  {
    return Optional.of(new Resource(getDataSource(), ResourceType.DATASOURCE));
  }
}
