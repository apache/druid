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

package org.apache.druid.msq.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Objects;

@JsonTypeName(InsertCannotAllocateSegmentFault.CODE)
public class InsertCannotAllocateSegmentFault extends BaseMSQFault
{
  static final String CODE = "InsertCannotAllocateSegment";

  private final String dataSource;
  private final Interval interval;

  @Nullable
  private final Interval allocatedInterval;

  @JsonCreator
  public InsertCannotAllocateSegmentFault(
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("interval") final Interval interval,
      @Nullable @JsonProperty("allocatedInterval") final Interval allocatedInterval
  )
  {
    super(CODE, getErrorMessage(dataSource, interval, allocatedInterval));
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.interval = Preconditions.checkNotNull(interval, "interval");
    this.allocatedInterval = allocatedInterval;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @Nullable
  @JsonProperty
  public Interval getAllocatedInterval()
  {
    return allocatedInterval;
  }

  private static String getErrorMessage(
      final String dataSource,
      final Interval interval,
      @Nullable final Interval allocatedInterval
  )
  {
    String errorMessage;
    if (allocatedInterval == null) {
      errorMessage = StringUtils.format(
          "Cannot allocate segment for dataSource [%s], interval [%s]. This can happen if the prior ingestion "
          + "uses non-extendable shard specs or if the partitioned by granularity is different from the granularity of the "
          + "pre-existing segments. Check the granularities of the pre-existing segments or re-run the ingestion with REPLACE "
          + "to overwrite over the existing data",
          dataSource,
          interval
      );
    } else {
      errorMessage = StringUtils.format(
          "Requested segment for dataSource [%s], interval [%s], but got [%s] interval instead. "
          + "This happens when an overlapping segment is already present with a coarser granularity for the requested interval. "
          + "Either set the partition granularity for the INSERT to [%s] to append to existing data or use REPLACE to "
          + "overwrite over the pre-existing segment",
          dataSource,
          interval,
          allocatedInterval,
          convertIntervalToGranularityString(allocatedInterval)
      );
    }
    return errorMessage;
  }

  /**
   * Converts the given interval to a string representing the granularity which is more user-friendly.
   */
  private static String convertIntervalToGranularityString(final Interval interval)
  {
    try {
      return GranularityType.fromPeriod(interval.toPeriod()).name();
    }
    catch (Exception e) {
      return new DurationGranularity(interval.toDurationMillis(), null).toString();
    }
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
    if (!super.equals(o)) {
      return false;
    }
    InsertCannotAllocateSegmentFault that = (InsertCannotAllocateSegmentFault) o;
    return Objects.equals(dataSource, that.dataSource)
           && Objects.equals(interval, that.interval)
           && Objects.equals(allocatedInterval, that.allocatedInterval);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), dataSource, interval, allocatedInterval);
  }
}
