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

package org.apache.druid.sql.calcite.expression;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.table.RowSignatures;

/**
 * Type of one operand of a {@link DefaultOperandTypeChecker}: a {@link SqlTypeFamily}, optionally with complex types
 * that are accepted too, whether NULL literals are accepted, and for {@link SqlTypeFamily#NUMERIC} operands, whether
 * character strings are accepted by casting them to numbers. Instances are immutable.
 */
public final class OperandType
{
  private final SqlTypeFamily family;
  private final ImmutableSet<ColumnType> complexTypes;
  private final boolean coerceStrings;
  private final boolean nullable;

  private OperandType(
      final SqlTypeFamily family,
      final ImmutableSet<ColumnType> complexTypes,
      final boolean coerceStrings,
      final boolean nullable
  )
  {
    this.family = Preconditions.checkNotNull(family, "family");
    this.complexTypes = complexTypes;
    this.coerceStrings = coerceStrings;
    this.nullable = nullable;
  }

  public static OperandType of(final SqlTypeFamily family)
  {
    return new OperandType(family, ImmutableSet.of(), false, false);
  }

  public static OperandType numeric()
  {
    return of(SqlTypeFamily.NUMERIC);
  }

  /**
   * Returns an operand type that also accepts the given complex types. Comparison uses
   * {@link RowSignatures.ComplexSqlType#getColumnType()}, so it does not depend on nullability. Not allowed for
   * {@link SqlTypeFamily#ANY} operands, which accept complex types already.
   */
  public OperandType orComplex(final ColumnType... types)
  {
    if (family == SqlTypeFamily.ANY) {
      throw DruidException.defensive("orComplex() does not apply to [%s] operands", SqlTypeFamily.ANY);
    }
    return new OperandType(
        family,
        ImmutableSet.<ColumnType>builder().addAll(complexTypes).add(types).build(),
        coerceStrings,
        nullable
    );
  }

  /**
   * Returns a {@link SqlTypeFamily#NUMERIC} operand type that also accepts character strings, such as string
   * columns, by casting them to DOUBLE. String literals that cannot be parsed as numbers are rejected, since they are
   * usually column names written in single quotes instead of double quotes. Datetime operands, such as
   * {@code __time}, are also accepted without a cast, since Druid stores them as numbers of milliseconds.
   */
  public OperandType coerceStrings()
  {
    if (family != SqlTypeFamily.NUMERIC) {
      throw DruidException.defensive(
          "coerceStrings() only applies to [%s] operands, got [%s]",
          SqlTypeFamily.NUMERIC,
          family
      );
    }
    return new OperandType(family, complexTypes, true, nullable);
  }

  /**
   * Returns an operand type that also accepts NULL literals, even if the operand is required. Optional operands
   * accept NULL literals already.
   */
  public OperandType nullable()
  {
    return new OperandType(family, complexTypes, coerceStrings, true);
  }

  public SqlTypeFamily getFamily()
  {
    return family;
  }

  public boolean isCoerceStrings()
  {
    return coerceStrings;
  }

  public boolean isNullable()
  {
    return nullable;
  }

  /**
   * Whether the type is one of the complex types given to {@link #orComplex(ColumnType...)}.
   */
  public boolean isAllowedComplexType(final RelDataType type)
  {
    return type instanceof RowSignatures.ComplexSqlType
           && complexTypes.contains(((RowSignatures.ComplexSqlType) type).getColumnType());
  }

  /**
   * Used by {@link DefaultOperandTypeChecker#getAllowedSignatures} if the operands have no names, e.g.
   * {@code NUMERIC | COMPLEX<tdigest>}.
   */
  @Override
  public String toString()
  {
    final StringBuilder sb = new StringBuilder(family.toString());
    for (final ColumnType type : complexTypes) {
      sb.append(" | ").append(type);
    }
    return sb.toString();
  }
}
