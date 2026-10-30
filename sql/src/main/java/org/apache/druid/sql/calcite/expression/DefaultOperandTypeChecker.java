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
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.calcite.util.Static;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.DruidTypeCoercion;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * Operand type checker that is used in 'simple' situations: there are a particular number of operands, with
 * particular types, some of which may be optional or nullable, and some of which may be required to be literals.
 */
public class DefaultOperandTypeChecker implements SqlOperandTypeChecker
{
  /**
   * Operand names for {@link #getAllowedSignatures(SqlOperator, String)}. May be empty, in which case the
   * {@link #operandTypes} are used instead.
   */
  private final List<String> operandNames;
  private final List<SqlTypeFamily> operandTypes;
  private final int requiredOperands;
  private final IntSet nullableOperands;
  private final IntSet literalOperands;

  /**
   * Complex types accepted by specific operands, in addition to the types of their {@link #operandTypes} family.
   * See {@link Builder#operandComplexTypes(int, ColumnType...)}.
   */
  private final Map<Integer, Set<ColumnType>> operandComplexTypes;

  /**
   * Numeric operands that accept character strings by coercing them to numbers, and datetimes as they are.
   * See {@link Builder#coercibleOperands(int...)}.
   */
  private final IntSet coercibleOperands;

  private DefaultOperandTypeChecker(
      final List<String> operandNames,
      final List<SqlTypeFamily> operandTypes,
      final int requiredOperands,
      final IntSet nullableOperands,
      @Nullable final int[] literalOperands,
      final Map<Integer, Set<ColumnType>> operandComplexTypes,
      final IntSet coercibleOperands
  )
  {
    Preconditions.checkArgument(requiredOperands <= operandTypes.size() && requiredOperands >= 0);
    this.operandNames = Preconditions.checkNotNull(operandNames, "operandNames");
    this.operandTypes = Preconditions.checkNotNull(operandTypes, "operandTypes");
    this.requiredOperands = requiredOperands;
    this.nullableOperands = Preconditions.checkNotNull(nullableOperands, "nullableOperands");

    if (!operandNames.isEmpty() && operandNames.size() != operandTypes.size()) {
      throw new ISE("Operand name count[%s] and type count[%s] must match", operandNames.size(), operandTypes.size());
    }

    if (literalOperands == null) {
      this.literalOperands = IntSets.EMPTY_SET;
    } else {
      this.literalOperands = new IntArraySet();
      Arrays.stream(literalOperands).forEach(this.literalOperands::add);
    }

    // Copy the sets too, so that later changes to the builder do not affect this checker.
    this.operandComplexTypes = new HashMap<>();
    Preconditions.checkNotNull(operandComplexTypes, "operandComplexTypes")
                 .forEach((operand, types) -> this.operandComplexTypes.put(operand, ImmutableSet.copyOf(types)));
    this.coercibleOperands = new IntArraySet(Preconditions.checkNotNull(coercibleOperands, "coercibleOperands"));

    for (final int operand : operandComplexTypes.keySet()) {
      if (operand < 0 || operand >= operandTypes.size()) {
        throw new ISE("Complex types given for operand[%s], but operand count is [%s]", operand, operandTypes.size());
      }
    }

    for (final int operand : coercibleOperands) {
      if (operand < 0 || operand >= operandTypes.size()) {
        throw new ISE("Coercible operand[%s] given, but operand count is [%s]", operand, operandTypes.size());
      }
      if (operandTypes.get(operand) != SqlTypeFamily.NUMERIC) {
        throw new ISE("Coercible operand[%s] must be of type family[%s]", operand, SqlTypeFamily.NUMERIC);
      }
    }
  }

  public static Builder builder()
  {
    return new Builder();
  }

  @Override
  public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure)
  {
    for (int i = 0; i < callBinding.operands().size(); i++) {
      final SqlNode operand = callBinding.operands().get(i);

      if (literalOperands.contains(i)) {
        // Verify that 'operand' is a literal. Allow CAST, since we can reduce these away later.
        if (!SqlUtil.isLiteral(operand, true)) {
          return OperatorConversions.throwOrReturn(
              throwOnFailure,
              callBinding,
              cb -> cb.getValidator()
                      .newValidationError(
                          operand,
                          Static.RESOURCE.argumentMustBeLiteral(callBinding.getOperator().getName())
                      )
          );
        }
      }

      if (coercibleOperands.contains(i) && operand instanceof SqlCharStringLiteral) {
        // Verify that a string literal can be converted to a number. This must happen before coercion, which would
        // otherwise turn it into a null number. Explicit CASTs are not checked, since they are the user's intent.
        final String value = ((SqlCharStringLiteral) operand).getValueAs(String.class);
        if (!Calcites.isNumericString(value)) {
          if (throwOnFailure) {
            throw Calcites.nonNumericStringLiteralError(
                StringUtils.format(
                    "Function [%s] argument [%s]",
                    callBinding.getOperator().getName(),
                    operandNames.isEmpty() ? i + 1 : operandNames.get(i)
                ),
                value
            );
          } else {
            return false;
          }
        }
      }

      final RelDataType operandType = callBinding.getValidator().deriveType(callBinding.getScope(), operand);
      final SqlTypeFamily expectedFamily = operandTypes.get(i);

      if (expectedFamily == SqlTypeFamily.ANY) {
        // ANY matches anything. This operand is all good; do nothing.
      } else if (expectedFamily.getTypeNames().contains(operandType.getSqlTypeName())) {
        // Operand came in with one of the expected types.
      } else if (isAllowedComplexType(i, operandType)) {
        // Operand came in with one of the complex types allowed for this operand.
      } else if (coercibleOperands.contains(i) && SqlTypeFamily.DATETIME.contains(operandType)) {
        // Operand came in with a datetime type, which Druid stores as a number of milliseconds.
      } else if (coercibleOperands.contains(i) && coerceToNumber(callBinding, i, operandType)) {
        // Operand was a character string, and is now cast to a number.
      } else if (operandType.getSqlTypeName() == SqlTypeName.NULL || SqlUtil.isNullLiteral(operand, true)) {
        // Null came in, check if operand is a nullable type.
        if (!nullableOperands.contains(i)) {
          return OperatorConversions.throwOrReturn(
              throwOnFailure,
              callBinding,
              cb -> cb.getValidator().newValidationError(operand, Static.RESOURCE.nullIllegal())
          );
        }
      } else {
        return OperatorConversions.throwOrReturn(
            throwOnFailure,
            callBinding,
            SqlCallBinding::newValidationSignatureError
        );
      }
    }

    return true;
  }

  private boolean isAllowedComplexType(final int operand, final RelDataType operandType)
  {
    final Set<ColumnType> allowedTypes = operandComplexTypes.get(operand);
    return allowedTypes != null
           && operandType instanceof RowSignatures.ComplexSqlType
           && allowedTypes.contains(((RowSignatures.ComplexSqlType) operandType).getColumnType());
  }

  /**
   * Casts a character string operand to DOUBLE using implicit type coercion. Returns false, without changing the
   * operand, if the operand is not a character string or if type coercion is not available.
   */
  private static boolean coerceToNumber(
      final SqlCallBinding callBinding,
      final int operand,
      final RelDataType operandType
  )
  {
    if (!SqlTypeFamily.CHARACTER.contains(operandType) || !callBinding.isTypeCoercionEnabled()) {
      return false;
    }

    final TypeCoercion typeCoercion = callBinding.getValidator().getTypeCoercion();
    if (!(typeCoercion instanceof DruidTypeCoercion)) {
      return false;
    }

    return ((DruidTypeCoercion) typeCoercion).coerceOperand(
        callBinding.getScope(),
        callBinding.getCall(),
        operand,
        Calcites.createSqlTypeWithNullability(
            callBinding.getTypeFactory(),
            SqlTypeName.DOUBLE,
            operandType.isNullable()
        )
    );
  }

  @Override
  public SqlOperandCountRange getOperandCountRange()
  {
    return SqlOperandCountRanges.between(requiredOperands, operandTypes.size());
  }

  @Override
  public String getAllowedSignatures(SqlOperator op, String opName)
  {
    final List<?> operands = !operandNames.isEmpty() ? operandNames : operandTypes;
    final StringBuilder ret = new StringBuilder();
    ret.append("'");
    ret.append(opName);
    ret.append("(");
    for (int i = 0; i < operands.size(); i++) {
      if (i > 0) {
        ret.append(", ");
      }
      if (i >= requiredOperands) {
        ret.append("[");
      }
      ret.append("<").append(operands.get(i)).append(">");
    }
    for (int i = requiredOperands; i < operands.size(); i++) {
      ret.append("]");
    }
    ret.append(")'");
    return ret.toString();
  }

  @Override
  public Consistency getConsistency()
  {
    return Consistency.NONE;
  }

  @Override
  public boolean isOptional(int i)
  {
    return i + 1 > requiredOperands;
  }

  public static class Builder
  {
    private List<String> operandNames = Collections.emptyList();
    private List<SqlTypeFamily> operandTypes;

    @Nullable
    private Integer requiredOperandCount;
    private int[] literalOperands;
    private IntSet notNullOperands = new IntArraySet();
    private final Map<Integer, Set<ColumnType>> operandComplexTypes = new HashMap<>();
    private final IntSet coercibleOperands = new IntArraySet();

    private Builder()
    {
    }

    public Builder operandNames(final String... operandNames)
    {
      this.operandNames = Arrays.asList(operandNames);
      return this;
    }

    public Builder operandNames(final List<String> operandNames)
    {
      this.operandNames = operandNames;
      return this;
    }

    public Builder operandTypes(final SqlTypeFamily... operandTypes)
    {
      this.operandTypes = Arrays.asList(operandTypes);
      return this;
    }

    public Builder operandTypes(final List<SqlTypeFamily> operandTypes)
    {
      this.operandTypes = operandTypes;
      return this;
    }

    public Builder requiredOperandCount(Integer requiredOperandCount)
    {
      this.requiredOperandCount = requiredOperandCount;
      return this;
    }

    public Builder literalOperands(final int... literalOperands)
    {
      this.literalOperands = literalOperands;
      return this;
    }

    public Builder notNullOperands(final int... notNullOperands)
    {
      Arrays.stream(notNullOperands).forEach(this.notNullOperands::add);
      return this;
    }

    /**
     * Allows the given complex types for an operand, in addition to the types of its type family. Comparison uses
     * {@link RowSignatures.ComplexSqlType#getColumnType()}, so it does not depend on nullability.
     */
    public Builder operandComplexTypes(final int operand, final ColumnType... complexTypes)
    {
      operandComplexTypes.computeIfAbsent(operand, k -> new HashSet<>()).addAll(Arrays.asList(complexTypes));
      return this;
    }

    /**
     * Marks {@link SqlTypeFamily#NUMERIC} operands that accept character strings, such as string columns, by casting
     * them to DOUBLE. String literals that cannot be parsed as numbers are rejected, since they are usually column
     * names written in single quotes instead of double quotes. Datetime operands, such as {@code __time}, are also
     * accepted without a cast, since Druid stores them as numbers of milliseconds.
     */
    public Builder coercibleOperands(final int... coercibleOperands)
    {
      Arrays.stream(coercibleOperands).forEach(this.coercibleOperands::add);
      return this;
    }

    public DefaultOperandTypeChecker build()
    {
      int computedRequiredOperandCount = requiredOperandCount == null ? operandTypes.size() : requiredOperandCount;
      return new DefaultOperandTypeChecker(
          operandNames,
          operandTypes,
          computedRequiredOperandCount,
          DefaultOperandTypeChecker.buildNullableOperands(computedRequiredOperandCount, operandTypes.size(), notNullOperands),
          literalOperands,
          operandComplexTypes,
          coercibleOperands
      );
    }
  }

  public static IntSet buildNullableOperands(int requiredOperandCount, int totalOperandCount, IntSet notNullOperands)
  {
    final IntSet nullableOperands = new IntArraySet();
    IntStream.range(requiredOperandCount, totalOperandCount)
             .filter(i -> !notNullOperands.contains(i))
             .forEach(nullableOperands::add);
    return nullableOperands;
  }
}
