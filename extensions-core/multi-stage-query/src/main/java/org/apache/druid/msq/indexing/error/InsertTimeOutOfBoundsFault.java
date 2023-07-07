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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.joda.time.Interval;

import java.util.List;
import java.util.Objects;

@JsonTypeName(InsertTimeOutOfBoundsFault.CODE)
public class InsertTimeOutOfBoundsFault extends BaseMSQFault
{
  static final String CODE = "InsertTimeOutOfBounds";

  private final Interval interval;
  private final List<Interval> intervalBounds;

  public InsertTimeOutOfBoundsFault(
      @JsonProperty("interval") Interval interval,
      @JsonProperty("intervalBounds") List<Interval> intervalBounds
  )
  {
    super(
        CODE,
        "Inserted data contains time chunk[%s] outside of bounds specified by OVERWRITE WHERE[%s]. "
        + "If you want to include this data, expand your OVERWRITE WHERE. "
        + "If you do not want to include this data, use SELECT ... WHERE to filter it from your inserted data.",
        interval,
        intervalBounds
    );
    this.interval = interval;
    this.intervalBounds = intervalBounds;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty
  public List<Interval> getIntervalBounds()
  {
    return intervalBounds;
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
    InsertTimeOutOfBoundsFault that = (InsertTimeOutOfBoundsFault) o;
    return Objects.equals(interval, that.interval) && Objects.equals(
        intervalBounds,
        that.intervalBounds
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), interval, intervalBounds);
  }

  @Override
  public String toString()
  {
    return "InsertTimeOutOfBoundsFault{" +
           "interval=" + interval +
           ", intervalBounds=" + intervalBounds +
           '}';
  }
}
