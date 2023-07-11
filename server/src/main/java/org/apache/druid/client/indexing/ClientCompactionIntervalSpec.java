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
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * InputSpec for {@link ClientCompactionIOConfig}.
 *
 * Should be synchronized with org.apache.druid.indexing.common.task.CompactionIntervalSpec.
 */
public class ClientCompactionIntervalSpec
{
  private static final Logger LOGGER = new Logger(ClientCompactionIntervalSpec.class);

  private static final String TYPE = "interval";

  private final Interval interval;
  @Nullable
  private final String sha256OfSortedSegmentIds;

  public static ClientCompactionIntervalSpec fromSegments(List<DataSegment> segments, @Nullable Granularity segmentGranularity)
  {
    Interval interval = JodaUtils.umbrellaInterval(segments.stream().map(DataSegment::getInterval).collect(Collectors.toList()));
    LOGGER.info("Original umbrella interval %s in compaction task for datasource %s", interval, segments.get(0).getDataSource());
    if (segmentGranularity != null) {
      // If segmentGranularity is set, then the segmentGranularity of the segments may not align with the configured segmentGranularity
      // We must adjust the interval of the compaction task to fully cover and align with the segmentGranularity
      // For example,
      // - The umbrella interval of the segments is 2015-04-11/2015-04-12 but configured segmentGranularity is YEAR,
      // if the compaction task's interval is 2015-04-11/2015-04-12 then we can run into race condition where after submitting
      // the compaction task, a new segment outside of the interval (i.e. 2015-02-11/2015-02-12) got created will be lost as it is
      // overshadowed by the compacted segment (compacted segment has interval 2015-01-01/2016-01-01.
      // Hence, in this case, we must adjust the compaction task interval to 2015-01-01/2016-01-01.
      // - The segment to be compacted has MONTH segmentGranularity with the interval 2015-02-01/2015-03-01 but configured
      // segmentGranularity is WEEK. If the compaction task's interval is 2015-02-01/2015-03-01 then compacted segments created will be
      // 2015-01-26/2015-02-02, 2015-02-02/2015-02-09, 2015-02-09/2015-02-16, 2015-02-16/2015-02-23, 2015-02-23/2015-03-02.
      // This is because Druid's WEEK segments alway start and end on Monday. In the above example, 2015-01-26 and 2015-03-02
      // are Mondays but 2015-02-01 and 2015-03-01 are not. Hence, the WEEK segments have to start and end on 2015-01-26 and 2015-03-02.
      // If the compaction task's interval is 2015-02-01/2015-03-01, then the compacted segment would cause existing data
      // from 2015-01-26 to 2015-02-01 and 2015-03-01 to 2015-03-02 to be lost. Hence, in this case,
      // we must adjust the compaction task interval to 2015-01-26/2015-03-02
      interval = JodaUtils.umbrellaInterval(segmentGranularity.getIterable(interval));
      LOGGER.info(
          "Interval adjusted to %s in compaction task for datasource %s with configured segmentGranularity %s",
          interval,
          segments.get(0).getDataSource(),
          segmentGranularity
      );
    }
    return new ClientCompactionIntervalSpec(
        interval,
        null
    );
  }

  @JsonCreator
  public ClientCompactionIntervalSpec(
      @JsonProperty("interval") Interval interval,
      @JsonProperty("sha256OfSortedSegmentIds") @Nullable String sha256OfSortedSegmentIds
  )
  {
    if (interval != null && interval.toDurationMillis() == 0) {
      throw new IAE("Interval[%s] is empty, must specify a nonempty interval", interval);
    }
    this.interval = interval;
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
           Objects.equals(sha256OfSortedSegmentIds, that.sha256OfSortedSegmentIds);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(interval, sha256OfSortedSegmentIds);
  }

  @Override
  public String toString()
  {
    return "ClientCompactionIntervalSpec{" +
           "interval=" + interval +
           ", sha256OfSortedSegmentIds='" + sha256OfSortedSegmentIds + '\'' +
           '}';
  }
}
