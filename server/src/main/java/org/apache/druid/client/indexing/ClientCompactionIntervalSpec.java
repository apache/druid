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

package org.apache.druid.client.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.SegmentDescriptor;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * InputSpec for {@link ClientCompactionIOConfig}.
 * <p>
 * Should be synchronized with org.apache.druid.indexing.common.task.CompactionIntervalSpec.
 */
public class ClientCompactionIntervalSpec
{
  private static final String TYPE = "interval";

  private final Interval interval;
  @Nullable
  private final List<SegmentDescriptor> uncompactedSegments;
  @Nullable
  private final String sha256OfSortedSegmentIds;

  @JsonCreator
  public ClientCompactionIntervalSpec(
      @JsonProperty("interval") Interval interval,
      @JsonProperty("uncompactedSegments") @Nullable List<SegmentDescriptor> uncompactedSegments,
      @JsonProperty("sha256OfSortedSegmentIds") @Nullable String sha256OfSortedSegmentIds
  )
  {
    if (interval != null && interval.toDurationMillis() == 0) {
      throw new IAE("Interval[%s] is empty, must specify a nonempty interval", interval);
    }
    this.interval = interval;
    if (uncompactedSegments == null) {
      // perform a full compaction
    } else if (uncompactedSegments.isEmpty()) {
      throw new IAE("Can not supply empty segments as input, please use either null or non-empty segments.");
    } else if (interval != null) {
      List<SegmentDescriptor> segmentsNotInInterval =
          uncompactedSegments.stream().filter(s -> !interval.contains(s.getInterval())).collect(Collectors.toList());
      if (!segmentsNotInInterval.isEmpty()) {
        throw new IAE(
            "Can not supply segments outside interval[%s], got segments[%s].",
            interval,
            segmentsNotInInterval
        );
      }
    }
    this.uncompactedSegments = uncompactedSegments;
    this.sha256OfSortedSegmentIds = sha256OfSortedSegmentIds;
  }

  @JsonProperty
  public String getType()
  {
    return TYPE;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<SegmentDescriptor> getUncompactedSegments()
  {
    return uncompactedSegments;
  }

  @Nullable
  @JsonProperty
  public String getSha256OfSortedSegmentIds()
  {
    return sha256OfSortedSegmentIds;
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
    ClientCompactionIntervalSpec that = (ClientCompactionIntervalSpec) o;
    return Objects.equals(interval, that.interval) &&
           Objects.equals(uncompactedSegments, that.uncompactedSegments) &&
           Objects.equals(sha256OfSortedSegmentIds, that.sha256OfSortedSegmentIds);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(interval, uncompactedSegments, sha256OfSortedSegmentIds);
  }

  @Override
  public String toString()
  {
    return "ClientCompactionIntervalSpec{" +
           "interval=" + interval +
           ", uncompactedSegments=" + uncompactedSegments +
           ", sha256OfSortedSegmentIds='" + sha256OfSortedSegmentIds + '\'' +
           '}';
  }
}
