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

package org.apache.druid.segment.projections;

import org.apache.druid.error.DruidException;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.NullableTypeStrategy;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Test-only utility: build {@link ClusteredValueGroupsBaseTableSchema} inputs from typed clustering tuples.
 * Derives the per-type dictionaries + per-spec dictionary IDs that the writer will emit at ingest time.
 */
public final class ClusterGroupSchemaTestHelpers
{
  private ClusterGroupSchemaTestHelpers()
  {
  }

  /** Bundle to pass into the {@link ClusteredValueGroupsBaseTableSchema} constructor's last two args. */
  public record Built(ClusteringDictionaries dictionaries, List<TableClusterGroupSpec> specs)
  {
  }

  /**
   * From typed clustering tuples (one per cluster group), build the per-type dictionaries and the matching specs.
   */
  public static Built buildClusterGroups(
      RowSignature clusteringColumns,
      List<? extends List<?>> tuples
  )
  {
    final int numCols = clusteringColumns.size();

    // 1. Coerce each tuple's raw values to the typed primitive expected per column.
    final List<Object[]> coercedTuples = new ArrayList<>(tuples.size());
    for (List<?> tuple : tuples) {
      if (tuple == null) {
        throw DruidException.defensive("tuple must not be null");
      }
      if (tuple.size() != numCols) {
        throw DruidException.defensive(
            "tuple size [%s] must match clusteringColumns size [%s]",
            tuple.size(),
            numCols
        );
      }
      final Object[] coerced = new Object[numCols];
      for (int i = 0; i < numCols; i++) {
        coerced[i] = coerceClusterGroupValue(
            clusteringColumns.getColumnName(i),
            clusteringColumns.getColumnType(i).orElseThrow(),
            tuple.get(i)
        );
      }
      coercedTuples.add(coerced);
    }

    // 2. Build per-type dictionaries; TreeSet on the type's strategy gives sort + dedupe in one shot.
    final Map<ValueType, TreeSet<Object>> perType = new EnumMap<>(ValueType.class);
    for (int i = 0; i < numCols; i++) {
      final ColumnType colType = clusteringColumns.getColumnType(i).orElseThrow();
      perType.computeIfAbsent(typeKey(colType), k -> newTypedTreeSet(colType));
    }
    for (Object[] coerced : coercedTuples) {
      for (int i = 0; i < numCols; i++) {
        final ColumnType colType = clusteringColumns.getColumnType(i).orElseThrow();
        perType.get(typeKey(colType)).add(coerced[i]);
      }
    }
    final List<String> stringDict = materialize(perType.get(ValueType.STRING));
    final List<Long> longDict = materialize(perType.get(ValueType.LONG));
    final List<Double> doubleDict = materialize(perType.get(ValueType.DOUBLE));
    final List<Float> floatDict = materialize(perType.get(ValueType.FLOAT));
    final ClusteringDictionaries dictionaries =
        new ClusteringDictionaries(stringDict, longDict, doubleDict, floatDict);

    // 3. For each tuple, look each value up in its column type's dictionary to derive the ID list.
    final List<TableClusterGroupSpec> specs = new ArrayList<>(coercedTuples.size());
    for (Object[] coerced : coercedTuples) {
      final List<Integer> ids = new ArrayList<>(numCols);
      for (int i = 0; i < numCols; i++) {
        final ColumnType colType = clusteringColumns.getColumnType(i).orElseThrow();
        @SuppressWarnings({"unchecked", "rawtypes"})
        final int id = Collections.binarySearch(
            (List) dictionaries.dictionaryForType(colType),
            coerced[i],
            (NullableTypeStrategy) colType.getNullableStrategy()
        );
        if (id < 0) {
          throw DruidException.defensive(
              "value [%s] not found in dictionary for clustering column [%s]; this is a bug in the helper",
              coerced[i],
              clusteringColumns.getColumnName(i)
          );
        }
        ids.add(id);
      }
      specs.add(new TableClusterGroupSpec(ids, null));
    }

    return new Built(dictionaries, Collections.unmodifiableList(specs));
  }

  private static ValueType typeKey(ColumnType columnType)
  {
    return columnType.getType();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static TreeSet<Object> newTypedTreeSet(ColumnType columnType)
  {
    return new TreeSet<>((NullableTypeStrategy) columnType.getNullableStrategy());
  }

  @SuppressWarnings("unchecked")
  private static <T> List<T> materialize(TreeSet<Object> set)
  {
    if (set == null || set.isEmpty()) {
      return List.of();
    }
    final List<T> out = new ArrayList<>(set.size());
    for (Object v : set) {
      out.add((T) v);
    }
    return Collections.unmodifiableList(out);
  }

  /**
   * Coerce a raw value (from JSON or test code) to the native Java type for {@code columnType}: STRING → String,
   * LONG → Long, DOUBLE → Double, FLOAT → Float. Null passes through; unsupported column types are rejected.
   */
  @Nullable
  public static Object coerceClusterGroupValue(
      String columnName,
      @Nullable ColumnType columnType,
      @Nullable Object raw
  )
  {
    if (!Projections.isAllowedClusteringType(columnType)) {
      throw DruidException.defensive(
          "clustering column [%s] has unsupported type [%s]; allowed types are STRING, LONG, DOUBLE, FLOAT",
          columnName,
          columnType
      );
    }
    if (raw == null) {
      return null;
    }
    if (columnType.is(ValueType.STRING)) {
      return DimensionHandlerUtils.convertObjectToString(raw);
    } else if (columnType.is(ValueType.LONG)) {
      return DimensionHandlerUtils.convertObjectToLong(raw);
    } else if (columnType.is(ValueType.DOUBLE)) {
      return DimensionHandlerUtils.convertObjectToDouble(raw);
    } else {
      return DimensionHandlerUtils.convertObjectToFloat(raw);
    }
  }
}
