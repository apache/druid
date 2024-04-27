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

package org.apache.druid.data.input;

import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
@PublicApi
public class MapBasedInputRow extends MapBasedRow implements InputRow
{
  private final List<String> dimensions;

  public MapBasedInputRow(
      long timestamp,
      List<String> dimensions,
      Map<String, Object> event
  )
  {
    super(timestamp, event);
    this.dimensions = dimensions;
  }

  public MapBasedInputRow(
      DateTime timestamp,
      List<String> dimensions,
      Map<String, Object> event
  )
  {
    super(timestamp, event);
    this.dimensions = dimensions;
  }

  @Override
  public List<String> getDimensions()
  {
    return dimensions;
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
    MapBasedInputRow that = (MapBasedInputRow) o;
    return Objects.equals(dimensions, that.dimensions);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), dimensions);
  }

  @Override
  public String toString()
  {
    return "{" +
           "timestamp=" + DateTimes.utc(getTimestampFromEpoch()) +
           ", event=" + getEvent() +
           ", dimensions=" + dimensions +
           '}';
  }
}
