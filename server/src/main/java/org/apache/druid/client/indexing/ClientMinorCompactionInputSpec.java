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
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.SegmentDescriptor;
import org.joda.time.Interval;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Client-side equivalent of {@code MinorCompactionInputSpec}.
 */
public class ClientMinorCompactionInputSpec extends ClientCompactionIntervalSpec
{
  public static final String TYPE = "minor";

  private final List<SegmentDescriptor> segments;

  @JsonCreator
  public ClientMinorCompactionInputSpec(
      @JsonProperty("interval") Interval interval,
      @JsonProperty("segments") List<SegmentDescriptor> segments
  )
  {
    super(interval, null);
    if (segments == null || segments.isEmpty()) {
      throw InvalidInput.exception("'segments' must be non-empty.");
    } else if (interval != null) {
      List<SegmentDescriptor> segmentsNotInInterval =
          segments.stream().filter(s -> !interval.contains(s.getInterval())).collect(Collectors.toList());
      if (!segmentsNotInInterval.isEmpty()) {
        throw new IAE(
            "Can not supply segments outside interval[%s], got segments[%s].",
            interval,
            segmentsNotInInterval
        );
      }
    }
    this.segments = segments;
  }

  @JsonProperty
  public List<SegmentDescriptor> getSegments()
  {
    return segments;
  }

  @Override
  public boolean equals(Object object)
  {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    if (!super.equals(object)) {
      return false;
    }
    ClientMinorCompactionInputSpec that = (ClientMinorCompactionInputSpec) object;
    return Objects.equals(segments, that.segments);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), segments);
  }

  @Override
  public String toString()
  {
    return "ClientMinorCompactionInputSpec{" +
           "interval=" + getInterval() +
           ",segments=" + segments +
           '}';
  }
}
