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

package org.apache.druid.segment.column;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Type signature for a row in a Druid datasource or query result. Rows have an ordering and every
 * column has a defined type. This is a little bit of a fiction in the Druid world (where rows do not _actually_ have
 * well defined types) but we do impose types for the SQL layer.
 *
 * @see org.apache.druid.query.QueryToolChest#resultArraySignature which returns signatures for query results
 * @see org.apache.druid.query.InlineDataSource#getRowSignature which returns signatures for inline datasources
 */
public class RowSignature
{
  private static final RowSignature EMPTY = new RowSignature(Collections.emptyList());

  private final Map<String, ValueType> columnTypes = new HashMap<>();
  private final Object2IntMap<String> columnPositions = new Object2IntOpenHashMap<>();
  private final List<String> columnNames;

  private RowSignature(final List<Pair<String, ValueType>> columnTypeList)
  {
    this.columnPositions.defaultReturnValue(-1);

    final ImmutableList.Builder<String> columnNamesBuilder = ImmutableList.builder();

    for (int i = 0; i < columnTypeList.size(); i++) {
      final Pair<String, ValueType> pair = columnTypeList.get(i);
      final ValueType existingType = columnTypes.get(pair.lhs);

      if (columnTypes.containsKey(pair.lhs) && existingType != pair.rhs) {
        // It's ok to add the same column twice as long as the type is consistent.
        // Note: we need the containsKey because the existingType might be present, but null.
        throw new IAE("Column[%s] has conflicting types [%s] and [%s]", pair.lhs, existingType, pair.rhs);
      }

      columnTypes.put(pair.lhs, pair.rhs);
      columnPositions.put(pair.lhs, i);
      columnNamesBuilder.add(pair.lhs);
    }

    this.columnNames = columnNamesBuilder.build();
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static RowSignature empty()
  {
    return EMPTY;
  }

  /**
   * Returns the name of the column at position {@code columnNumber}.
   *
   * @throws IndexOutOfBoundsException if columnNumber is not within our row length
   */
  public String getColumnName(final int columnNumber)
  {
    return columnNames.get(columnNumber);
  }

  /**
   * Returns the type of the column named {@code columnName}, or empty if the type is unknown or the column does
   * not exist.
   */
  public Optional<ValueType> getColumnType(final String columnName)
  {
    return Optional.ofNullable(columnTypes.get(columnName));
  }

  /**
   * Returns the type of the column at position {@code columnNumber}, or empty if the type is unknown.
   *
   * @throws IndexOutOfBoundsException if columnNumber is not within our row length
   */
  public Optional<ValueType> getColumnType(final int columnNumber)
  {
    return Optional.ofNullable(columnTypes.get(getColumnName(columnNumber)));
  }

  /**
   * Returns a list of column names in the order they appear in this signature.
   */
  public List<String> getColumnNames()
  {
    return columnNames;
  }

  /**
   * Returns the number of columns in this signature.
   */
  public int size()
  {
    return columnNames.size();
  }

  /**
   * Returns whether this signature contains a named column.
   */
  public boolean contains(final String columnName)
  {
    return columnPositions.containsKey(columnName);
  }

  /**
   * Returns the first position of {@code columnName} in this row signature, or -1 if it does not appear.
   *
   * Note: the same column name may appear more than once in a signature; if it does, this method will return the
   * first appearance.
   */
  public int indexOf(final String columnName)
  {
    return columnPositions.applyAsInt(columnName);
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
    RowSignature that = (RowSignature) o;
    return columnTypes.equals(that.columnTypes) &&
           columnNames.equals(that.columnNames);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(columnTypes, columnNames);
  }

  @Override
  public String toString()
  {
    final StringBuilder s = new StringBuilder("{");
    for (int i = 0; i < columnNames.size(); i++) {
      if (i > 0) {
        s.append(", ");
      }
      final String columnName = columnNames.get(i);
      s.append(columnName).append(":").append(columnTypes.get(columnName));
    }
    return s.append("}").toString();
  }

  public static class Builder
  {
    private final List<Pair<String, ValueType>> columnTypeList;

    private Builder()
    {
      this.columnTypeList = new ArrayList<>();
    }

    /**
     * Add a column to this signature.
     *
     * @param columnName name, must be nonnull
     * @param columnType type, may be null if unknown
     */
    public Builder add(final String columnName, @Nullable final ValueType columnType)
    {
      // Name must be nonnull, but type can be null (if the type is unknown)
      Preconditions.checkNotNull(columnName, "'columnName' must be nonnull");
      columnTypeList.add(Pair.of(columnName, columnType));
      return this;
    }

    public Builder addAll(final RowSignature other)
    {
      for (String columnName : other.getColumnNames()) {
        add(columnName, other.getColumnType(columnName).orElse(null));
      }

      return this;
    }

    public Builder addTimeColumn()
    {
      return add(ColumnHolder.TIME_COLUMN_NAME, ValueType.LONG);
    }

    public Builder addDimensions(final List<DimensionSpec> dimensions)
    {
      for (final DimensionSpec dimension : dimensions) {
        add(dimension.getOutputName(), dimension.getOutputType());
      }

      return this;
    }

    public Builder addAggregators(final List<AggregatorFactory> aggregators)
    {
      for (final AggregatorFactory aggregator : aggregators) {
        final ValueType type = GuavaUtils.getEnumIfPresent(
            ValueType.class,
            StringUtils.toUpperCase(aggregator.getTypeName())
        );

        // Use null instead of COMPLEX for nonnumeric types, since in that case, the type depends on whether or not
        // the aggregator is finalized, and we don't know (a) if it will be finalized, or even (b) what the type would
        // be if it were finalized. So null (i.e. unknown) is the proper thing to do.
        //
        // Another note: technically, we don't know what the finalized type will be even if the type here is numeric,
        // but we're assuming that it doesn't change upon finalization. All builtin aggregators work this way.

        if (type != null && type.isNumeric()) {
          add(aggregator.getName(), type);
        } else {
          add(aggregator.getName(), null);
        }
      }

      return this;
    }

    public Builder addPostAggregators(final List<PostAggregator> postAggregators)
    {
      for (final PostAggregator postAggregator : postAggregators) {
        // PostAggregator#getName is marked nullable, but we require column names for everything.
        final String name = Preconditions.checkNotNull(
            postAggregator.getName(),
            "postAggregators must have nonnull names"
        );

        // PostAggregators don't have known types; use null for the type.
        add(name, null);
      }

      return this;
    }

    public RowSignature build()
    {
      return new RowSignature(columnTypeList);
    }
  }
}
