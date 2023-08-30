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
import org.apache.calcite.runtime.CalciteException;
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

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * Operand type checker that is used in simple situations: there are a particular number of operands, with
 * particular types, some of which may be optional or nullable, and some of which may be required to be literals.
 */
public class BasicOperandTypeChecker implements SqlOperandTypeChecker
{
  private final List<SqlTypeFamily> operandTypes;
  private final int requiredOperands;
  private final IntSet nullOperands;
  private final IntSet literalOperands;

  BasicOperandTypeChecker(
      final List<SqlTypeFamily> operandTypes,
      final int requiredOperands,
      final IntSet nullOperands,
      @Nullable final int[] literalOperands
  )
  {
    Preconditions.checkArgument(requiredOperands <= operandTypes.size() && requiredOperands >= 0);
    this.operandTypes = Preconditions.checkNotNull(operandTypes, "operandTypes");
    this.requiredOperands = requiredOperands;
    this.nullOperands = Preconditions.checkNotNull(nullOperands, "nullOperands");

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

  public static boolean throwOrReturn(
      final boolean throwOnFailure,
      final SqlCallBinding callBinding,
      final Function<SqlCallBinding, CalciteException> exceptionMapper
  )
  {
    if (throwOnFailure) {
      throw exceptionMapper.apply(callBinding);
    } else {
      return false;
    }
  }

  @Override
  public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure)
  {
    for (int i = 0; i < callBinding.operands().size(); i++) {
      final SqlNode operand = callBinding.operands().get(i);

      if (literalOperands.contains(i)) {
        // Verify that 'operand' is a literal.
        if (!SqlUtil.isLiteral(operand)) {
          return throwOrReturn(
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
        if (!nullOperands.contains(i)) {
          return throwOrReturn(
              throwOnFailure,
              callBinding,
              cb -> cb.getValidator().newValidationError(operand, Static.RESOURCE.nullIllegal())
          );
        }
      } else {
        return throwOrReturn(
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
    return SqlUtil.getAliasedSignature(op, opName, operandTypes);
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
    private List<SqlTypeFamily> operandTypes;
    private Integer requiredOperandCount = null;
    private int[] literalOperands = null;

    /**
     * Signifies that a function accepts operands of type family given by {@param operandTypes}.
     */
    public Builder operandTypes(final SqlTypeFamily... operandTypes)
    {
      this.operandTypes = Arrays.asList(operandTypes);
      return this;
    }

    /**
     * Signifies that a function accepts operands of type family given by {@param operandTypes}.
     */
    public Builder operandTypes(final List<SqlTypeFamily> operandTypes)
    {
      this.operandTypes = operandTypes;
      return this;
    }

    /**
     * Signifies that the first {@code requiredOperands} operands are required, and all later operands are optional.
     *
     * Required operands are not allowed to be null. Optional operands can either be skipped or explicitly provided as
     * literal NULLs. For example, if {@code requiredOperands == 1}, then {@code F(x, NULL)} and  {@code F(x)} are both
     * accepted, and {@code x} must not be null.
     */
    public Builder requiredOperandCount(final int requiredOperandCount)
    {
      this.requiredOperandCount = requiredOperandCount;
      return this;
    }

    /**
     * Signifies that the operands at positions given by {@code literalOperands} must be literals.
     */
    public Builder literalOperands(final int... literalOperands)
    {
      this.literalOperands = literalOperands;
      return this;
    }

    public BasicOperandTypeChecker build()
    {
      // Create "nullableOperands" set including all optional arguments.
      final IntSet nullableOperands = new IntArraySet();
      if (requiredOperandCount != null) {
        IntStream.range(requiredOperandCount, operandTypes.size()).forEach(nullableOperands::add);
      }

      return new BasicOperandTypeChecker(
          operandTypes,
          requiredOperandCount == null ? operandTypes.size() : requiredOperandCount,
          nullableOperands,
          literalOperands
      );
    }
  }
}
