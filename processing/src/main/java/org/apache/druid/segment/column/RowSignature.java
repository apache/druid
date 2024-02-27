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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnInspector;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Type signature for a row in a Druid datasource or query result.
 *
 * @see org.apache.druid.query.QueryToolChest#resultArraySignature which returns signatures for query results
 * @see InlineDataSource#getRowSignature which returns signatures for inline datasources
 */
public class RowSignature implements ColumnInspector
{
  private static final RowSignature EMPTY = new RowSignature(Collections.emptyList());

  private final Map<String, ColumnType> columnTypes = new HashMap<>();
  private final Object2IntMap<String> columnPositions = new Object2IntOpenHashMap<>();
  private final List<String> columnNames;

  /**
   * Precompute and store the hashCode since it is getting interned in
   * {@link org.apache.druid.sql.calcite.schema.DruidSchema}
   * Also helps in comparing the RowSignatures in equals method
   */
  private final int hashCode;

  private RowSignature(final List<ColumnSignature> columnTypeList)
  {
    this.columnPositions.defaultReturnValue(-1);

    final ImmutableList.Builder<String> columnNamesBuilder = ImmutableList.builder();

    for (int i = 0; i < columnTypeList.size(); i++) {
      final ColumnSignature sig = columnTypeList.get(i);
      final ColumnType existingType = columnTypes.get(sig.name());

      if (columnTypes.containsKey(sig.name()) && !Objects.equals(existingType, sig.type())) {
        // It's ok to add the same column twice as long as the type is consistent.
        // Note: we need the containsKey because the existingType might be present, but null.
        throw new IAE("Column[%s] has conflicting types [%s] and [%s]", sig.name(), existingType, sig.type());
      }

      columnTypes.put(sig.name(), sig.type());
      columnPositions.put(sig.name(), i);
      columnNamesBuilder.add(sig.name());
    }

    this.columnNames = columnNamesBuilder.build();
    this.hashCode = computeHashCode();
  }

  @JsonCreator
  static RowSignature fromColumnSignatures(final List<ColumnSignature> columnSignatures)
  {
    final Builder builder = builder();

    for (final ColumnSignature columnSignature : columnSignatures) {
      builder.add(columnSignature.name(), columnSignature.type());
    }

    return builder.build();
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
  public Optional<ColumnType> getColumnType(final String columnName)
  {
    return Optional.ofNullable(columnTypes.get(columnName));
  }

  /**
   * Returns the type of the column at position {@code columnNumber}, or empty if the type is unknown.
   *
   * @throws IndexOutOfBoundsException if columnNumber is not within our row length
   */
  public Optional<ColumnType> getColumnType(final int columnNumber)
  {
    return Optional.ofNullable(columnTypes.get(getColumnName(columnNumber)));
  }

  /**
   * Returns true if the column is a numeric type ({@link ColumnType#isNumeric()}), otherwise false if the column
   * is not a numeric type or is not present in the row signature.
   */
  public boolean isNumeric(final String columnName)
  {
    return getColumnType(columnName).map(ColumnType::isNumeric).orElse(false);
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

  public boolean contains(final int columnNumber)
  {
    return 0 <= columnNumber && columnNumber < columnNames.size();
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

  @JsonValue
  private List<ColumnSignature> asColumnSignatures()
  {
    final List<ColumnSignature> retVal = new ArrayList<>();

    for (String columnName : columnNames) {
      final ColumnType type = columnTypes.get(columnName);
      retVal.add(new ColumnSignature(columnName, type));
    }

    return retVal;
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
    return hashCode == that.hashCode &&
           columnTypes.equals(that.columnTypes) &&
           columnNames.equals(that.columnNames);
  }

  private int computeHashCode()
  {
    return Objects.hash(columnTypes, columnNames);
  }

  @Override
  public int hashCode()
  {
    return hashCode;
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

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return getColumnType(column).map(columnType -> {
      if (columnType.isNumeric()) {
        return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(columnType);
      } else if (columnType.is(ValueType.COMPLEX)) {
        return ColumnCapabilitiesImpl.createDefault().setType(columnType).setHasNulls(true);
      } else {
        return new ColumnCapabilitiesImpl().setType(columnType);
      }
    }).orElse(null);
  }

  public static class Builder
  {
    private final List<ColumnSignature> columnTypeList;

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
    public Builder add(final String columnName, @Nullable final ColumnType columnType)
    {
      columnTypeList.add(new ColumnSignature(columnName, columnType));
      return this;
    }

    public Builder addAll(final RowSignature other)
    {
      final List<String> names = other.getColumnNames();
      for (int i = 0; i < names.size(); i++) {
        add(names.get(i), other.getColumnType(i).orElse(null));
      }

      return this;
    }

    public Builder addTimeColumn()
    {
      return add(ColumnHolder.TIME_COLUMN_NAME, ColumnType.LONG);
    }

    public Builder addDimensions(final List<DimensionSpec> dimensions)
    {
      for (final DimensionSpec dimension : dimensions) {
        add(dimension.getOutputName(), dimension.getOutputType());
      }

      return this;
    }

    /**
     * Adds aggregations to a signature.
     *
     * {@link Finalization#YES} will add finalized types and {@link Finalization#NO} will add intermediate types.
     * {@link Finalization#UNKNOWN} will add the intermediate / finalized type when they are the same. Otherwise, it
     * will add a null type.
     *
     * @param aggregators  list of aggregation functions
     * @param finalization whether the aggregator results will be finalized
     */
    public Builder addAggregators(final List<AggregatorFactory> aggregators, final Finalization finalization)
    {
      for (final AggregatorFactory aggregator : aggregators) {
        final ColumnType type;

        switch (finalization) {
          case YES:
            type = aggregator.getResultType();
            break;

          case NO:
            type = aggregator.getIntermediateType();
            break;

          default:
            assert finalization == Finalization.UNKNOWN;

            if (aggregator.getIntermediateType().equals(aggregator.getResultType())) {
              type = aggregator.getIntermediateType();
            } else {
              // Use null if the type depends on whether the aggregator is finalized, since we don't know if
              // it will be finalized or not.
              type = null;
            }
            break;
        }

        add(aggregator.getName(), type);
      }

      return this;
    }

    /**
     * Adds post-aggregators to a signature.
     *
     * Note: to ensure types are computed properly, post-aggregators must be added *after* any columns that they
     * depend on, and they must be added in the order that the query engine will compute them. This method assumes
     * that post-aggregators are computed in order, and that they can refer to earlier post-aggregators but not
     * to later ones.
     */
    public Builder addPostAggregators(final List<PostAggregator> postAggregators)
    {
      for (final PostAggregator postAggregator : postAggregators) {
        // PostAggregator#getName is marked nullable, but we require column names for everything.
        final String name = Preconditions.checkNotNull(
            postAggregator.getName(),
            "postAggregators must have nonnull names"
        );

        // It's OK to call getType in the order that post-aggregators appear, because post-aggregators are only
        // allowed to refer to *earlier* post-aggregators (not later ones; the order is meaningful).
        add(name, postAggregator.getType(build()));
      }

      return this;
    }

    public RowSignature build()
    {
      return new RowSignature(columnTypeList);
    }
  }

  public enum Finalization
  {
    /**
     * Aggregation results will be finalized.
     */
    YES,

    /**
     * Aggregation results will not be finalized.
     */
    NO,

    /**
     * Aggregation results may or may not be finalized.
     */
    UNKNOWN
  }

  /**
   * Builds a safe {@link RowSignature}.
   *
   * The new rowsignature will not contain `null` types - they will be replaced by STRING.
   */
  public RowSignature buildSafeSignature(ImmutableList<String> requestedColumnNames)
  {
    Builder builder = new Builder();
    for (String columnName : requestedColumnNames) {
      ColumnType columnType = columnTypes.get(columnName);
      if (columnType == null) {
        columnType = ColumnType.STRING;
      }
      builder.add(columnName, columnType);
    }
    return builder.build();
  }

  /**
   * Returns the column types in the order they are in.
   */
  public List<ColumnType> getColumnTypes()
  {
    List<ColumnType> ret = new ArrayList<ColumnType>();
    for (String colName : columnNames) {
      ret.add(columnTypes.get(colName));
    }
    return ret;
  }
}
