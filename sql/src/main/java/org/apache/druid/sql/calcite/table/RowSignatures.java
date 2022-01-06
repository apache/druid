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

package org.apache.druid.sql.calcite.table;

import com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.AbstractSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.expression.SimpleExtraction;
import org.apache.druid.sql.calcite.planner.Calcites;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Utility functions for working with {@link RowSignature}.
 */
public class RowSignatures
{
  private RowSignatures()
  {
    // No instantiation.
  }

  public static RowSignature fromRelDataType(final List<String> rowOrder, final RelDataType rowType)
  {
    if (rowOrder.size() != rowType.getFieldCount()) {
      throw new IAE("Field count %d != %d", rowOrder.size(), rowType.getFieldCount());
    }

    final RowSignature.Builder rowSignatureBuilder = RowSignature.builder();

    for (int i = 0; i < rowOrder.size(); i++) {
      final RelDataType dataType = rowType.getFieldList().get(i).getType();
      final ColumnType valueType = Calcites.getColumnTypeForRelDataType(dataType);
      if (valueType == null) {
        throw new ISE(
            "Cannot translate sqlTypeName[%s] to Druid type for field[%s]",
            dataType.getSqlTypeName(),
            rowOrder.get(i)
        );
      }

      rowSignatureBuilder.add(rowOrder.get(i), valueType);
    }

    return rowSignatureBuilder.build();
  }

  /**
   * Return the "natural" {@link StringComparator} for an extraction from a row signature. This will be a
   * lexicographic comparator for String types and a numeric comparator for Number types.
   */
  @Nonnull
  public static StringComparator getNaturalStringComparator(
      final RowSignature rowSignature,
      final SimpleExtraction simpleExtraction
  )
  {
    Preconditions.checkNotNull(simpleExtraction, "simpleExtraction");
    if (simpleExtraction.getExtractionFn() != null
        || rowSignature.getColumnType(simpleExtraction.getColumn()).map(type -> type.is(ValueType.STRING)).orElse(false)) {
      return StringComparators.LEXICOGRAPHIC;
    } else {
      return StringComparators.NUMERIC;
    }
  }

  /**
   * Returns a Calcite RelDataType corresponding to a row signature.
   */
  public static RelDataType toRelDataType(final RowSignature rowSignature, final RelDataTypeFactory typeFactory)
  {
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    final boolean nullNumeric = !NullHandling.replaceWithDefault();
    for (final String columnName : rowSignature.getColumnNames()) {
      final RelDataType type;

      if (ColumnHolder.TIME_COLUMN_NAME.equals(columnName)) {
        type = Calcites.createSqlType(typeFactory, SqlTypeName.TIMESTAMP);
      } else {
        final ColumnType columnType =
            rowSignature.getColumnType(columnName)
                        .orElseThrow(() -> new ISE("Encountered null type for column[%s]", columnName));

        switch (columnType.getType()) {
          case STRING:
            // Note that there is no attempt here to handle multi-value in any special way. Maybe one day...
            type = Calcites.createSqlTypeWithNullability(typeFactory, SqlTypeName.VARCHAR, true);
            break;
          case LONG:
            type = Calcites.createSqlTypeWithNullability(typeFactory, SqlTypeName.BIGINT, nullNumeric);
            break;
          case FLOAT:
            type = Calcites.createSqlTypeWithNullability(typeFactory, SqlTypeName.FLOAT, nullNumeric);
            break;
          case DOUBLE:
            type = Calcites.createSqlTypeWithNullability(typeFactory, SqlTypeName.DOUBLE, nullNumeric);
            break;
          case ARRAY:
            switch (columnType.getElementType().getType()) {
              case STRING:
                type = Calcites.createSqlArrayTypeWithNullability(typeFactory, SqlTypeName.VARCHAR, true);
                break;
              case LONG:
                type = Calcites.createSqlArrayTypeWithNullability(typeFactory, SqlTypeName.BIGINT, nullNumeric);
                break;
              case DOUBLE:
                type = Calcites.createSqlArrayTypeWithNullability(typeFactory, SqlTypeName.DOUBLE, nullNumeric);
                break;
              default:
                throw new ISE("valueType[%s] not translatable", columnType);
            }
            break;
          case COMPLEX:
            type = typeFactory.createTypeWithNullability(
                new ComplexSqlType(SqlTypeName.OTHER, columnType, true),
                true
            );
            break;
          default:
            throw new ISE("valueType[%s] not translatable", columnType);
        }
      }

      builder.add(columnName, type);
    }

    return builder.build();
  }

  /**
   * Calcite {@link RelDataType} for Druid complex columns, to preserve complex type information
   */
  public static final class ComplexSqlType extends AbstractSqlType
  {
    private final ColumnType columnType;

    public ComplexSqlType(
        SqlTypeName typeName,
        ColumnType columnType,
        boolean isNullable
    )
    {
      super(typeName, isNullable, null);
      this.columnType = columnType;
      this.computeDigest();
    }

    @Override
    public RelDataTypeComparability getComparability()
    {
      return RelDataTypeComparability.UNORDERED;
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail)
    {
      sb.append(columnType.asTypeString());
    }

    public String getComplexTypeName()
    {
      return columnType.getComplexTypeName();
    }

    public String asTypeString()
    {
      return columnType.asTypeString();
    }
  }
}
