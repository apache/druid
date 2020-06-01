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

package org.apache.druid.indexing.common.task.batch.parallel.distribution;

import java.util.Objects;

/**
 * Tuple of timestamp and dimension value
 */
public class TimeDimTuple implements Comparable<TimeDimTuple>
{
  private final long timestamp;
  private final String dimensionValue;

  TimeDimTuple(long timestamp, String dimensionValue)
  {
    this.timestamp = timestamp;
    this.dimensionValue = dimensionValue;
  }

  public long getTimestamp()
  {
    return timestamp;
  }

  public String getDimensionValue()
  {
    return dimensionValue;
  }

  @Override
  public int compareTo(TimeDimTuple o)
  {
    if (timestamp < o.timestamp) {
      return -1;
    }

    if (o.timestamp < timestamp) {
      return 1;
    }

    return dimensionValue.compareTo(o.dimensionValue);
  }

  @Override
  public boolean equals(Object o)
  {
    if (!(o instanceof TimeDimTuple)) {
      return false;
    }
    return compareTo((TimeDimTuple) o) == 0;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(timestamp, dimensionValue);
  }

  @Override
  public String toString()
  {
    return "TimeDimTuple{" +
           "timestamp=" + timestamp +
           ", dimensionValue='" + dimensionValue + '\'' +
           '}';
  }
}

