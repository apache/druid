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
import org.joda.time.Interval;

import java.util.Objects;

@JsonTypeName(InsertCannotAllocateSegmentFault.CODE)
public class InsertCannotAllocateSegmentFault extends BaseMSQFault
{
  static final String CODE = "InsertCannotAllocateSegment";

  private final String dataSource;
  private final Interval interval;

  @JsonCreator
  public InsertCannotAllocateSegmentFault(
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("interval") final Interval interval
  )
  {
    super(CODE, "Cannot allocate segment for dataSource [%s], interval [%s]", dataSource, interval);
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.interval = Preconditions.checkNotNull(interval, "interval");
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
    return Objects.equals(dataSource, that.dataSource) && Objects.equals(interval, that.interval);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), dataSource, interval);
  }
}
