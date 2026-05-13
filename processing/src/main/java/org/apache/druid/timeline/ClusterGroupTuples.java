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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.segment.VirtualColumns;
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
 * {@link #getClusteringColumns()}. Optionally carries the clustering {@link VirtualColumns} when the segment was
 * clustered on a virtual-column expression, so that matching for things like partial load rules and query time
 * segment pruning can make use of this information.
 */
public class ClusterGroupTuples
{
  private final RowSignature clusteringColumns;
  private final List<List<Object>> tuples;
  private final VirtualColumns virtualColumns;

  @JsonCreator
  public ClusterGroupTuples(
      @JsonProperty("clusteringColumns") RowSignature clusteringColumns,
      @JsonProperty("tuples") @Nullable List<List<Object>> tuples,
      @JsonProperty("virtualColumns") @Nullable VirtualColumns virtualColumns
  )
  {
    if (clusteringColumns == null || clusteringColumns.size() == 0) {
      throw InvalidInput.exception("clusteringColumns must not be null or empty");
    }
    this.clusteringColumns = clusteringColumns;
    this.virtualColumns = internVirtualColumns(virtualColumns);

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

  /**
   * Convenience constructor for callers that don't carry clustering virtual columns. Equivalent to passing
   * {@code null} for the virtual columns argument.
   */
  public ClusterGroupTuples(RowSignature clusteringColumns, @Nullable List<List<Object>> tuples)
  {
    this(clusteringColumns, tuples, null);
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

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  private static VirtualColumns internVirtualColumns(@Nullable VirtualColumns virtualColumns)
  {
    if (virtualColumns == null || virtualColumns.isEmpty()) {
      return VirtualColumns.EMPTY;
    }
    return VirtualColumns.create(
        Arrays.stream(virtualColumns.getVirtualColumns())
              .map(DataSegment.virtualColumnInterner()::intern)
              .toList()
    );
  }

  /**
   * Canonicalize {@code raw} for the declared clustering column {@code type}. This is intentionally narrow: its job
   * is to unbreak Jackson's number-type narrowing (e.g., an Integer arriving for a LONG column gets normalized to a
   * Long), not to do general value coercion. Rules:
   * <ul>
   *   <li>{@code null} → {@code null}.</li>
   *   <li>STRING: {@link Objects#toString} on any non-null value (stringifying numeric operator input is benign).</li>
   *   <li>LONG / DOUBLE / FLOAT: require {@link Number}; return via the matching primitive accessor. Strings,
   *       Booleans, etc. are rejected — typed rule authoring should produce typed JSON, and silently parsing
   *       strings risks accepting operator typos that change the matched set.</li>
   * </ul>
   * Unsupported column types (anything that isn't STRING/LONG/DOUBLE/FLOAT) are rejected.
   * <p>
   * Used by:
   * <ul>
   *   <li>{@link ClusterGroupTuples}'s constructor to canonicalize segment-side tuples (strict).</li>
   *   <li>Operator-supplied rule tuples in future cluster-group partial-load matchers, which can catch the
   *       exception and treat it as "no match for this segment" rather than a hard failure.</li>
   * </ul>
   */
  @Nullable
  public static Object coerceValue(String columnName, ColumnType type, @Nullable Object raw)
  {
    if (raw == null) {
      return null;
    }
    if (ColumnType.STRING.equals(type)) {
      return raw instanceof String ? raw : Objects.toString(raw);
    }
    if (ColumnType.LONG.equals(type)) {
      if (raw instanceof Number) {
        return ((Number) raw).longValue();
      }
      throw cannotCoerce(raw, columnName, "LONG");
    }
    if (ColumnType.DOUBLE.equals(type)) {
      if (raw instanceof Number) {
        return ((Number) raw).doubleValue();
      }
      throw cannotCoerce(raw, columnName, "DOUBLE");
    }
    if (ColumnType.FLOAT.equals(type)) {
      if (raw instanceof Number) {
        return ((Number) raw).floatValue();
      }
      throw cannotCoerce(raw, columnName, "FLOAT");
    }
    throw InvalidInput.exception(
        "Unsupported clustering column type [%s] for column [%s]; supported types are STRING, LONG, DOUBLE, FLOAT",
        type,
        columnName
    );
  }

  private static DruidException cannotCoerce(Object raw, String columnName, String targetType)
  {
    return InvalidInput.exception(
        "Cannot coerce value [%s] of type [%s] for column [%s] to %s",
        raw,
        raw.getClass().getName(),
        columnName,
        targetType
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
    return Objects.equals(clusteringColumns, that.clusteringColumns)
           && Objects.equals(tuples, that.tuples)
           && Objects.equals(virtualColumns, that.virtualColumns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(clusteringColumns, tuples, virtualColumns);
  }

  @Override
  public String toString()
  {
    return "ClusterGroupTuples{clusteringColumns=" + clusteringColumns
           + ", tuples=" + tuples
           + ", virtualColumns=" + virtualColumns
           + '}';
  }
}
