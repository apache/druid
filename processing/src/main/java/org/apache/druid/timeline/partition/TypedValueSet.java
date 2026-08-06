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

package org.apache.druid.timeline.partition;

import org.apache.druid.segment.column.ColumnType;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * A type-carrying set of stringified match values, produced by
 * {@link org.apache.druid.query.filter.DimFilter#getDimensionValueSet(String)} and consumed by
 * {@link ShardSpec#possibleInValueDomain(java.util.Map)}.
 *
 * <p>The value-domain analog of the {@code RangeSet<String>} used by
 * {@link org.apache.druid.query.filter.DimFilter#getDimensionRangeSet(String)} /
 * {@link ShardSpec#possibleInDomain(java.util.Map)}, but it also carries the filter's match {@link ColumnType}. That
 * type is the safety gate for numeric segment pruning: a {@link DimensionValueSetShardSpec} may only prune when the
 * filter's match type equals the type stamped for the dimension, because value stringification only agrees across
 * ingest and query for identical types (LONG {@code 1} → {@code "1"}, but DOUBLE {@code 1.0} → {@code "1.0"}). A
 * typeless {@code RangeSet<String>} cannot express this gate, which is why this is a separate channel.
 *
 * <p>The contained {@link #values} are the stringified match values (via {@code Evals.asString}), matching the
 * ingest side. The set may contain a {@code null} element to denote a null/missing match value (e.g. an {@code IN}
 * list containing {@code NULL}).
 */
public class TypedValueSet
{
  private final Set<String> values;
  private final ColumnType type;

  public TypedValueSet(Set<String> values, ColumnType type)
  {
    // Defensive copy into a set that tolerates a null element (denoting a null match value).
    this.values = Collections.unmodifiableSet(new HashSet<>(values));
    this.type = type;
  }

  public Set<String> getValues()
  {
    return values;
  }

  public ColumnType getType()
  {
    return type;
  }

  public boolean contains(String value)
  {
    return values.contains(value);
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
    TypedValueSet that = (TypedValueSet) o;
    return Objects.equals(values, that.values) && Objects.equals(type, that.type);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(values, type);
  }

  @Override
  public String toString()
  {
    return "TypedValueSet{values=" + values + ", type=" + type + '}';
  }
}
