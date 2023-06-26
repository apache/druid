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
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.joda.time.Interval;

import java.util.Objects;

@JsonTypeName(InsertAllocatedIncorrectSegmentFault.CODE)
public class InsertAllocatedIncorrectSegmentFault extends BaseMSQFault
{
  static final String CODE = "InsertAllocatedIncorrectSegment";

  private final String dataSource;
  private final Interval requestedInterval;
  private final Interval allocatedInterval;

  @JsonCreator
  public InsertAllocatedIncorrectSegmentFault(
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("requestedInterval") final Interval requestedInterval,
      @JsonProperty("allocatedInterval") final Interval allocatedInterval
  )
  {
    super(
        CODE,
        "Requested segment for dataSource [%s], interval [%s], but got [%s] interval instead. "
        + "This happens when an overlapping segment is already present with a coarser granularity for the requested interval. "
        + "Either set the partition granularity for the INSERT to [%s] to append to existing data or use REPLACE to "
        + "overwrite over the pre-existing segment",
        dataSource,
        requestedInterval,
        allocatedInterval,
        convertIntervalToGranularityString(allocatedInterval)
    );
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.requestedInterval = Preconditions.checkNotNull(requestedInterval, "requestedInterval");
    this.allocatedInterval = Preconditions.checkNotNull(allocatedInterval, "allocatedInterval");
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public Interval getRequestedInterval()
  {
    return requestedInterval;
  }

  @JsonProperty
  public Interval getAllocatedInterval()
  {
    return allocatedInterval;
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
    InsertAllocatedIncorrectSegmentFault that = (InsertAllocatedIncorrectSegmentFault) o;
    return Objects.equals(dataSource, that.dataSource)
           && Objects.equals(requestedInterval, that.requestedInterval)
           && Objects.equals(allocatedInterval, that.allocatedInterval);
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
  public int hashCode()
  {
    return Objects.hash(dataSource, requestedInterval, allocatedInterval);
  }
}
