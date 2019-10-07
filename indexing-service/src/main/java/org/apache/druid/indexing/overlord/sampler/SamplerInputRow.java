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

package org.apache.druid.indexing.overlord.sampler;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Row;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class SamplerInputRow implements InputRow
{
  public static final String SAMPLER_ORDERING_COLUMN = "__internal_sampler_order";

  private final InputRow row;
  private final int sortKey;

  public SamplerInputRow(InputRow row, int sortKey)
  {
    this.row = row;
    this.sortKey = sortKey;
  }

  @Override
  public List<String> getDimensions()
  {
    return row.getDimensions();
  }

  @Override
  public long getTimestampFromEpoch()
  {
    return row.getTimestampFromEpoch();
  }

  @Override
  public DateTime getTimestamp()
  {
    return row.getTimestamp();
  }

  @Override
  public List<String> getDimension(String dimension)
  {
    return row.getDimension(dimension);
  }

  @Nullable
  @Override
  public Object getRaw(String dimension)
  {
    return SAMPLER_ORDERING_COLUMN.equals(dimension) ? sortKey : row.getRaw(dimension);
  }

  @Nullable
  @Override
  public Number getMetric(String metric)
  {
    return SAMPLER_ORDERING_COLUMN.equals(metric) ? sortKey : row.getMetric(metric);
  }

  @Override
  public int compareTo(Row o)
  {
    return row.compareTo(o);
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
    SamplerInputRow that = (SamplerInputRow) o;
    return sortKey == that.sortKey && Objects.equals(row, that.row);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(row, sortKey);
  }
}
