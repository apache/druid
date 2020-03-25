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

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Represents an inline datasource, where the rows are embedded within the DataSource object itself.
 *
 * The rows are backed by an Iterable, which can be lazy or not. Lazy datasources will only be iterated if someone calls
 * {@link #getRows()} and iterates the result, or until someone calls {@link #getRowsAsList()}.
 */
public class InlineDataSource implements DataSource
{
  private final Iterable<Object[]> rows;
  private final RowSignature signature;

  private InlineDataSource(
      final Iterable<Object[]> rows,
      final RowSignature signature
  )
  {
    this.rows = Preconditions.checkNotNull(rows, "'rows' must be nonnull");
    this.signature = Preconditions.checkNotNull(signature, "'signature' must be nonnull");
  }

  /**
   * Factory method for Jackson. Used for inline datasources that were originally encoded as JSON. Private because
   * non-Jackson callers should use {@link #fromIterable}.
   */
  @JsonCreator
  private static InlineDataSource fromJson(
      @JsonProperty("columnNames") List<String> columnNames,
      @JsonProperty("columnTypes") List<ValueType> columnTypes,
      @JsonProperty("rows") List<Object[]> rows
  )
  {
    Preconditions.checkNotNull(columnNames, "'columnNames' must be nonnull");

    if (columnTypes != null && columnNames.size() != columnTypes.size()) {
      throw new IAE("columnNames and columnTypes must be the same length");
    }

    final RowSignature.Builder builder = RowSignature.builder();

    for (int i = 0; i < columnNames.size(); i++) {
      final String name = columnNames.get(i);
      final ValueType type = columnTypes != null ? columnTypes.get(i) : null;
      builder.add(name, type);
    }

    return new InlineDataSource(rows, builder.build());
  }

  /**
   * Creates an inline datasource from an Iterable. The Iterable will not be iterated until someone calls
   * {@link #getRows()} and iterates the result, or until someone calls {@link #getRowsAsList()}.
   *
   * @param rows      rows, each of the same length as {@code signature.size()}
   * @param signature row signature
   */
  public static InlineDataSource fromIterable(
      final Iterable<Object[]> rows,
      final RowSignature signature
  )
  {
    return new InlineDataSource(rows, signature);
  }

  @Override
  public Set<String> getTableNames()
  {
    return Collections.emptySet();
  }

  @JsonProperty
  public List<String> getColumnNames()
  {
    return signature.getColumnNames();
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<ValueType> getColumnTypes()
  {
    if (IntStream.range(0, signature.size()).noneMatch(i -> signature.getColumnType(i).isPresent())) {
      // All types are null; return null for columnTypes so it doesn't show up in serialized JSON.
      return null;
    } else {
      return IntStream.range(0, signature.size())
                      .mapToObj(i -> signature.getColumnType(i).orElse(null))
                      .collect(Collectors.toList());
    }
  }

  /**
   * Returns rows as a list. If the original Iterable behind this datasource was a List, this method will return it
   * as-is, without copying it. Otherwise, this method will walk the iterable and copy it into a List before returning.
   */
  @JsonProperty("rows")
  public List<Object[]> getRowsAsList()
  {
    return rows instanceof List ? ((List<Object[]>) rows) : Lists.newArrayList(rows);
  }

  /**
   * Returns rows as an Iterable.
   */
  @JsonIgnore
  public Iterable<Object[]> getRows()
  {
    return rows;
  }

  @Override
  public List<DataSource> getChildren()
  {
    return Collections.emptyList();
  }

  @Override
  public DataSource withChildren(List<DataSource> children)
  {
    if (!children.isEmpty()) {
      throw new IAE("Cannot accept children");
    }

    return this;
  }

  @Override
  public boolean isCacheable()
  {
    return false;
  }

  @Override
  public boolean isGlobal()
  {
    return true;
  }

  @Override
  public boolean isConcrete()
  {
    return true;
  }

  /**
   * Returns the row signature (map of column name to type) for this inline datasource. Note that types may
   * be null, meaning we know we have a column with a certain name, but we don't know what its type is.
   */
  public RowSignature getRowSignature()
  {
    return signature;
  }

  public RowAdapter<Object[]> rowAdapter()
  {
    return new RowAdapter<Object[]>()
    {
      @Override
      public ToLongFunction<Object[]> timestampFunction()
      {
        final int columnNumber = signature.indexOf(ColumnHolder.TIME_COLUMN_NAME);

        if (columnNumber >= 0) {
          return row -> (long) row[columnNumber];
        } else {
          return row -> 0L;
        }
      }

      @Override
      public Function<Object[], Object> columnFunction(String columnName)
      {
        final int columnNumber = signature.indexOf(columnName);

        if (columnNumber >= 0) {
          return row -> row[columnNumber];
        } else {
          return row -> null;
        }
      }
    };
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
    InlineDataSource that = (InlineDataSource) o;
    return rowsEqual(rows, that.rows) &&
           Objects.equals(signature, that.signature);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(rowsHashCode(rows), signature);
  }

  @Override
  public String toString()
  {
    // Don't include 'rows' in stringification, because it might be long and/or lazy.
    return "InlineDataSource{" +
           "signature=" + signature +
           '}';
  }

  /**
   * A very zealous equality checker for "rows" that respects deep equality of arrays, but nevertheless refrains
   * from materializing things needlessly. Useful for unit tests that want to compare equality of different
   * InlineDataSource instances.
   */
  private static boolean rowsEqual(final Iterable<Object[]> rowsA, final Iterable<Object[]> rowsB)
  {
    if (rowsA instanceof List && rowsB instanceof List) {
      final List<Object[]> listA = (List<Object[]>) rowsA;
      final List<Object[]> listB = (List<Object[]>) rowsB;

      if (listA.size() != listB.size()) {
        return false;
      }

      for (int i = 0; i < listA.size(); i++) {
        final Object[] rowA = listA.get(i);
        final Object[] rowB = listB.get(i);

        if (!Arrays.equals(rowA, rowB)) {
          return false;
        }
      }

      return true;
    } else {
      return Objects.equals(rowsA, rowsB);
    }
  }

  /**
   * A very zealous hash code computer for "rows" that is compatible with {@link #rowsEqual}.
   */
  private static int rowsHashCode(final Iterable<Object[]> rows)
  {
    if (rows instanceof List) {
      final List<Object[]> list = (List<Object[]>) rows;

      int code = 1;
      for (final Object[] row : list) {
        code = 31 * code + Arrays.hashCode(row);
      }

      return code;
    } else {
      return Objects.hash(rows);
    }
  }
}
