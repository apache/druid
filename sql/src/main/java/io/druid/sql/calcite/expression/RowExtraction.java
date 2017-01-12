/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.expression;

import com.google.common.base.Preconditions;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.column.Column;
import io.druid.segment.column.ValueType;
import io.druid.segment.filter.Filters;
import io.druid.sql.calcite.rel.DruidQueryBuilder;
import io.druid.sql.calcite.table.RowSignature;

/**
 * Represents an extraction of a value from a Druid row. Can be used for grouping, filtering, etc.
 *
 * Currently this is a column plus an extractionFn, but it's expected that as time goes on, this will become more
 * general and allow for variously-typed extractions from multiple columns.
 */
public class RowExtraction
{
  private final String column;
  private final ExtractionFn extractionFn;

  public RowExtraction(String column, ExtractionFn extractionFn)
  {
    this.column = Preconditions.checkNotNull(column, "column");
    this.extractionFn = extractionFn;
  }

  public static RowExtraction of(String column, ExtractionFn extractionFn)
  {
    return new RowExtraction(column, extractionFn);
  }

  public static RowExtraction fromDimensionSpec(final DimensionSpec dimensionSpec)
  {
    if (dimensionSpec instanceof ExtractionDimensionSpec) {
      return RowExtraction.of(
          dimensionSpec.getDimension(),
          ((ExtractionDimensionSpec) dimensionSpec).getExtractionFn()
      );
    } else if (dimensionSpec instanceof DefaultDimensionSpec) {
      return RowExtraction.of(dimensionSpec.getDimension(), null);
    } else {
      return null;
    }
  }

  public static RowExtraction fromQueryBuilder(
      final DruidQueryBuilder queryBuilder,
      final int fieldNumber
  )
  {
    final String fieldName = queryBuilder.getRowOrder().get(fieldNumber);

    if (queryBuilder.getGrouping() != null) {
      for (DimensionSpec dimensionSpec : queryBuilder.getGrouping().getDimensions()) {
        if (dimensionSpec.getOutputName().equals(fieldName)) {
          return RowExtraction.fromDimensionSpec(dimensionSpec);
        }
      }

      return null;
    } else if (queryBuilder.getSelectProjection() != null) {
      for (DimensionSpec dimensionSpec : queryBuilder.getSelectProjection().getDimensions()) {
        if (dimensionSpec.getOutputName().equals(fieldName)) {
          return RowExtraction.fromDimensionSpec(dimensionSpec);
        }
      }

      for (String metricName : queryBuilder.getSelectProjection().getMetrics()) {
        if (metricName.equals(fieldName)) {
          return RowExtraction.of(metricName, null);
        }
      }

      return null;
    } else {
      // No select projection or grouping.
      return RowExtraction.of(queryBuilder.getRowOrder().get(fieldNumber), null);
    }
  }

  public String getColumn()
  {
    return column;
  }

  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  /**
   * Check if this extraction can be used to build a filter on a Druid dataSource. This method exists because we can't
   * filter on floats (yet) and things like DruidFilterRule need to check for that.
   *
   * @param rowSignature row signature of the dataSource
   *
   * @return whether or not this extraction is filterable
   */
  public boolean isFilterable(final RowSignature rowSignature)
  {
    return Filters.FILTERABLE_TYPES.contains(rowSignature.getColumnType(column));
  }

  public DimensionSpec toDimensionSpec(final RowSignature rowSignature, final String outputName)
  {
    final ValueType columnType = rowSignature.getColumnType(column);
    if (columnType == null) {
      return null;
    }

    if (columnType == ValueType.STRING || (column.equals(Column.TIME_COLUMN_NAME) && extractionFn != null)) {
      return extractionFn == null
             ? new DefaultDimensionSpec(column, outputName)
             : new ExtractionDimensionSpec(column, outputName, extractionFn);
    } else {
      // Can't create dimensionSpecs for non-string, non-time.
      return null;
    }
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

    RowExtraction that = (RowExtraction) o;

    if (column != null ? !column.equals(that.column) : that.column != null) {
      return false;
    }
    return extractionFn != null ? extractionFn.equals(that.extractionFn) : that.extractionFn == null;

  }

  @Override
  public int hashCode()
  {
    int result = column != null ? column.hashCode() : 0;
    result = 31 * result + (extractionFn != null ? extractionFn.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    if (extractionFn != null) {
      return String.format("%s(%s)", extractionFn, column);
    } else {
      return column;
    }
  }
}
