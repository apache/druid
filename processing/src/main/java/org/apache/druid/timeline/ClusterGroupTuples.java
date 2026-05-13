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

package org.apache.druid.timeline;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Typed clustering tuples carried on {@link DataSegment#getClusterGroups()} for clustered base-table segments. Each
 * entry in {@link #getTuples()} is one cluster group's clustering-column values, in the order declared by
 * {@link #getClusteringColumns()}.
 */
public class ClusterGroupTuples
{
  private final RowSignature clusteringColumns;
  private final List<List<Object>> tuples;

  @JsonCreator
  public ClusterGroupTuples(
      @JsonProperty("clusteringColumns") RowSignature clusteringColumns,
      @JsonProperty("tuples") @Nullable List<List<Object>> tuples
  )
  {
    if (clusteringColumns == null || clusteringColumns.size() == 0) {
      throw InvalidInput.exception("clusteringColumns must not be null or empty");
    }
    this.clusteringColumns = clusteringColumns;

    final List<List<Object>> source = tuples == null ? Collections.emptyList() : tuples;
    final int numCols = clusteringColumns.size();
    final List<List<Object>> coerced = new ArrayList<>(source.size());
    for (int t = 0; t < source.size(); t++) {
      final List<Object> tuple = source.get(t);
      if (tuple == null || tuple.size() != numCols) {
        throw InvalidInput.exception(
            "tuple[%s] has size [%s] but clusteringColumns size is [%s]",
            t,
            tuple == null ? "null" : tuple.size(),
            numCols
        );
      }
      final Object[] out = new Object[numCols];
      for (int i = 0; i < numCols; i++) {
        final String name = clusteringColumns.getColumnName(i);
        final ColumnType type = clusteringColumns.getColumnType(i).orElseThrow(
            () -> InvalidInput.exception("clusteringColumn[%s] has no declared type", name)
        );
        out[i] = coerceValue(name, type, tuple.get(i));
      }
      coerced.add(Collections.unmodifiableList(Arrays.asList(out)));
    }
    this.tuples = Collections.unmodifiableList(coerced);
  }

  @JsonProperty
  public RowSignature getClusteringColumns()
  {
    return clusteringColumns;
  }

  @JsonProperty
  public List<List<Object>> getTuples()
  {
    return tuples;
  }

  /**
   * Coerce {@code raw} into the canonical Java representation of {@code type}. Null in, null out. STRING uses
   * {@link String#valueOf}; LONG / DOUBLE / FLOAT accept any {@link Number} (down/up-cast via the matching primitive
   * accessor) or a parseable numeric {@link String}. Throws on unsupported types or unparseable strings.
   * <p>
   * Used by:
   * <ul>
   *   <li>{@link ClusterGroupTuples}'s constructor to canonicalize segment-side tuples (strict).</li>
   *   <li>Operator-supplied rule tuples in the cluster-group partial-load matchers, which catch the exception and
   *       treat it as "no match for this segment" rather than a hard failure.</li>
   * </ul>
   */
  public static Object coerceValue(String columnName, ColumnType type, @Nullable Object raw)
  {
    if (raw == null) {
      return null;
    }
    if (ColumnType.STRING.equals(type)) {
      return raw instanceof String ? raw : String.valueOf(raw);
    }
    if (ColumnType.LONG.equals(type)) {
      if (raw instanceof Number) {
        return ((Number) raw).longValue();
      }
      if (raw instanceof String) {
        try {
          return Long.parseLong((String) raw);
        }
        catch (NumberFormatException e) {
          throw InvalidInput.exception("Cannot coerce value [%s] for column [%s] to LONG", raw, columnName);
        }
      }
      throw InvalidInput.exception("Cannot coerce value [%s] for column [%s] to LONG", raw, columnName);
    }
    if (ColumnType.DOUBLE.equals(type)) {
      if (raw instanceof Number) {
        return ((Number) raw).doubleValue();
      }
      if (raw instanceof String) {
        try {
          return Double.parseDouble((String) raw);
        }
        catch (NumberFormatException e) {
          throw InvalidInput.exception("Cannot coerce value [%s] for column [%s] to DOUBLE", raw, columnName);
        }
      }
      throw InvalidInput.exception("Cannot coerce value [%s] for column [%s] to DOUBLE", raw, columnName);
    }
    if (ColumnType.FLOAT.equals(type)) {
      if (raw instanceof Number) {
        return ((Number) raw).floatValue();
      }
      if (raw instanceof String) {
        try {
          return Float.parseFloat((String) raw);
        }
        catch (NumberFormatException e) {
          throw InvalidInput.exception("Cannot coerce value [%s] for column [%s] to FLOAT", raw, columnName);
        }
      }
      throw InvalidInput.exception("Cannot coerce value [%s] for column [%s] to FLOAT", raw, columnName);
    }
    throw InvalidInput.exception(
        "Unsupported clustering column type [%s] for column [%s]; supported types are STRING, LONG, DOUBLE, FLOAT",
        type,
        columnName
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ClusterGroupTuples)) {
      return false;
    }
    ClusterGroupTuples that = (ClusterGroupTuples) o;
    return Objects.equals(clusteringColumns, that.clusteringColumns) && Objects.equals(tuples, that.tuples);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(clusteringColumns, tuples);
  }

  @Override
  public String toString()
  {
    return "ClusterGroupTuples{clusteringColumns=" + clusteringColumns + ", tuples=" + tuples + '}';
  }
}
