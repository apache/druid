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

package org.apache.druid.sql.calcite.filtration;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Objects;

public class RangeValue implements Comparable<RangeValue>
{
  @Nullable
  private final Object value;
  private final ColumnType matchValueType;
  private final Comparator<Object> matchValueTypeComparator;

  public RangeValue(
      @Nullable Object value,
      ColumnType matchValueType
  )
  {
    this.value = value;
    this.matchValueType = matchValueType;
    this.matchValueTypeComparator = matchValueType.getNullableStrategy();
  }

  @Nullable
  public Object getValue()
  {
    return value;
  }

  @Override
  public int compareTo(RangeValue o)
  {
    if (!matchValueType.equals(o.matchValueType)) {
      throw new ISE("Comparator mismatch: [%s] and [%s]", matchValueType, o.matchValueType);
    }
    return matchValueTypeComparator.compare(value, o.value);
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
    RangeValue that = (RangeValue) o;
    return Objects.equals(value, that.value) && Objects.equals(matchValueType, that.matchValueType);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(value, matchValueType);
  }

  @Override
  public String toString()
  {
    return "RangeValue{" +
           "value=" + value +
           ", matchValueType=" + matchValueType +
           '}';
  }
}
