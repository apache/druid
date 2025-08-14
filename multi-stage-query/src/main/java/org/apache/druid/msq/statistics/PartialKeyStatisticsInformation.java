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

package org.apache.druid.msq.statistics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Set;

/**
 * Class sent by worker to controller after reading input to generate partition boundries.
 */
public class PartialKeyStatisticsInformation
{
  private final Set<Long> timeSegments;

  private final boolean multipleValues;

  private final double bytesRetained;

  @JsonCreator
  public PartialKeyStatisticsInformation(
      @JsonProperty("timeSegments") Set<Long> timeSegments,
      @JsonProperty("multipleValues") boolean hasMultipleValues,
      @JsonProperty("bytesRetained") double bytesRetained
  )
  {
    this.timeSegments = timeSegments;
    this.multipleValues = hasMultipleValues;
    this.bytesRetained = bytesRetained;
  }

  @JsonProperty("timeSegments")
  public Set<Long> getTimeSegments()
  {
    return timeSegments;
  }

  @JsonProperty("multipleValues")
  public boolean hasMultipleValues()
  {
    return multipleValues;
  }

  @JsonProperty("bytesRetained")
  public double getBytesRetained()
  {
    return bytesRetained;
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
    PartialKeyStatisticsInformation that = (PartialKeyStatisticsInformation) o;
    return multipleValues == that.multipleValues
           && Double.compare(bytesRetained, that.bytesRetained) == 0
           && Objects.equals(timeSegments, that.timeSegments);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(timeSegments, multipleValues, bytesRetained);
  }

  @Override
  public String toString()
  {
    return "PartialKeyStatisticsInformation{" +
           "timeSegments=" + timeSegments +
           ", multipleValues=" + multipleValues +
           ", bytesRetained=" + bytesRetained +
           '}';
  }
}
