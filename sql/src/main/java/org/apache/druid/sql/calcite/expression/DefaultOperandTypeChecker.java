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
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Static;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

  private DefaultOperandTypeChecker(
      final List<String> operandNames,
      final List<SqlTypeFamily> operandTypes,
      final int requiredOperands,
      final IntSet nullableOperands,
      @Nullable final int[] literalOperands
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

      final RelDataType operandType = callBinding.getValidator().deriveType(callBinding.getScope(), operand);
      final SqlTypeFamily expectedFamily = operandTypes.get(i);

      if (expectedFamily == SqlTypeFamily.ANY) {
        // ANY matches anything. This operand is all good; do nothing.
      } else if (expectedFamily.getTypeNames().contains(operandType.getSqlTypeName())) {
        // Operand came in with one of the expected types.
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

    public DefaultOperandTypeChecker build()
    {
      int computedRequiredOperandCount = requiredOperandCount == null ? operandTypes.size() : requiredOperandCount;
      return new DefaultOperandTypeChecker(
          operandNames,
          operandTypes,
          computedRequiredOperandCount,
          DefaultOperandTypeChecker.buildNullableOperands(computedRequiredOperandCount, operandTypes.size(), notNullOperands),
          literalOperands
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
