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
import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Static;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignature;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * Utilities for assisting in writing {@link SqlOperatorConversion} implementations.
 */
public class OperatorConversions
{
  @Nullable
  public static DruidExpression convertCall(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode,
      final String functionName
  )
  {
    return convertCall(
        plannerContext,
        rowSignature,
        rexNode,
        druidExpressions -> DruidExpression.fromFunctionCall(functionName, druidExpressions)
    );
  }

  @Nullable
  public static DruidExpression convertCall(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode,
      final String functionName,
      final Function<List<DruidExpression>, SimpleExtraction> simpleExtractionFunction
  )
  {
    return convertCall(
        plannerContext,
        rowSignature,
        rexNode,
        druidExpressions -> DruidExpression.of(
            simpleExtractionFunction == null ? null : simpleExtractionFunction.apply(druidExpressions),
            DruidExpression.functionCall(functionName, druidExpressions)
        )
    );
  }

  @Nullable
  public static DruidExpression convertCall(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode,
      final Function<List<DruidExpression>, DruidExpression> expressionFunction
  )
  {
    final RexCall call = (RexCall) rexNode;

    final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
        plannerContext,
        rowSignature,
        call.getOperands()
    );

    if (druidExpressions == null) {
      return null;
    }

    return expressionFunction.apply(druidExpressions);
  }

  /**
   * Gets operand "i" from "operands", or returns a default value if it doesn't exist (operands is too short)
   * or is null.
   */
  public static <T> T getOperandWithDefault(
      final List<RexNode> operands,
      final int i,
      final Function<RexNode, T> f,
      final T defaultReturnValue
  )
  {
    if (operands.size() > i && !RexLiteral.isNullLiteral(operands.get(i))) {
      return f.apply(operands.get(i));
    } else {
      return defaultReturnValue;
    }
  }

  @Nullable
  public static DruidExpression convertCallWithPostAggOperands(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode,
      final Function<List<DruidExpression>, DruidExpression> expressionFunction,
      final PostAggregatorVisitor postAggregatorVisitor
  )
  {
    final RexCall call = (RexCall) rexNode;

    final List<DruidExpression> druidExpressions = Expressions.toDruidExpressionsWithPostAggOperands(
        plannerContext,
        rowSignature,
        call.getOperands(),
        postAggregatorVisitor
    );

    if (druidExpressions == null) {
      return null;
    }

    return expressionFunction.apply(druidExpressions);
  }

  /**
   * Translate a Calcite {@code RexNode} to a Druid PostAggregator
   *
   * @param plannerContext SQL planner context
   * @param rowSignature   signature of the rows to be extracted from
   * @param rexNode        expression meant to be applied on top of the rows
   *
   * @param postAggregatorVisitor visitor that manages postagg names and tracks postaggs that were created
   *                              by the translation
   * @return rexNode referring to fields in rowOrder, or null if not possible
   */
  @Nullable
  public static PostAggregator toPostAggregator(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode,
      final PostAggregatorVisitor postAggregatorVisitor
  )
  {
    final SqlKind kind = rexNode.getKind();
    if (kind == SqlKind.INPUT_REF) {
      // Translate field references.
      final RexInputRef ref = (RexInputRef) rexNode;
      final String columnName = rowSignature.getRowOrder().get(ref.getIndex());
      if (columnName == null) {
        throw new ISE("WTF?! PostAgg referred to nonexistent index[%d]", ref.getIndex());
      }

      return new FieldAccessPostAggregator(
          postAggregatorVisitor.getOutputNamePrefix() + postAggregatorVisitor.getAndIncrementCounter(),
          columnName
      );
    } else if (rexNode instanceof RexCall) {
      final SqlOperator operator = ((RexCall) rexNode).getOperator();
      final SqlOperatorConversion conversion = plannerContext.getOperatorTable()
                                                             .lookupOperatorConversion(operator);

      if (conversion == null) {
        return null;
      } else {
        return conversion.toPostAggregator(
            plannerContext,
            rowSignature,
            rexNode,
            postAggregatorVisitor
        );
      }
    } else if (kind == SqlKind.LITERAL) {
      return null;
    } else {
      throw new IAE("Unknown rexnode kind: " + kind);
    }
  }

  public static OperatorBuilder operatorBuilder(final String name)
  {
    return new OperatorBuilder(name);
  }

  public static class OperatorBuilder
  {
    private final String name;
    private SqlKind kind = SqlKind.OTHER_FUNCTION;
    private SqlReturnTypeInference returnTypeInference;
    private SqlFunctionCategory functionCategory = SqlFunctionCategory.USER_DEFINED_FUNCTION;

    // For operand type checking
    private SqlOperandTypeChecker operandTypeChecker;
    private List<SqlTypeFamily> operandTypes;
    private Integer requiredOperands = null;

    private OperatorBuilder(final String name)
    {
      this.name = Preconditions.checkNotNull(name, "name");
    }

    public OperatorBuilder kind(final SqlKind kind)
    {
      this.kind = kind;
      return this;
    }

    public OperatorBuilder returnType(final SqlTypeName typeName)
    {
      this.returnTypeInference = ReturnTypes.explicit(
          factory -> Calcites.createSqlType(factory, typeName)
      );
      return this;
    }

    public OperatorBuilder nullableReturnType(final SqlTypeName typeName)
    {
      this.returnTypeInference = ReturnTypes.explicit(
          factory -> Calcites.createSqlTypeWithNullability(factory, typeName, true)
      );
      return this;
    }

    public OperatorBuilder returnTypeInference(final SqlReturnTypeInference returnTypeInference)
    {
      this.returnTypeInference = returnTypeInference;
      return this;
    }

    public OperatorBuilder functionCategory(final SqlFunctionCategory functionCategory)
    {
      this.functionCategory = functionCategory;
      return this;
    }

    public OperatorBuilder operandTypeChecker(final SqlOperandTypeChecker operandTypeChecker)
    {
      this.operandTypeChecker = operandTypeChecker;
      return this;
    }

    public OperatorBuilder operandTypes(final SqlTypeFamily... operandTypes)
    {
      this.operandTypes = Arrays.asList(operandTypes);
      return this;
    }

    public OperatorBuilder requiredOperands(final int requiredOperands)
    {
      this.requiredOperands = requiredOperands;
      return this;
    }

    public SqlFunction build()
    {
      // Create "nullableOperands" set including all optional arguments.
      final IntSet nullableOperands = new IntArraySet();
      if (requiredOperands != null) {
        IntStream.range(requiredOperands, operandTypes.size()).forEach(nullableOperands::add);
      }

      final SqlOperandTypeChecker theOperandTypeChecker;

      if (operandTypeChecker == null) {
        theOperandTypeChecker = new DefaultOperandTypeChecker(
            operandTypes,
            requiredOperands == null ? operandTypes.size() : requiredOperands,
            nullableOperands
        );
      } else if (operandTypes == null && requiredOperands == null) {
        theOperandTypeChecker = operandTypeChecker;
      } else {
        throw new ISE(
            "Cannot have both 'operandTypeChecker' and 'operandTypes' / 'requiredOperands'"
        );
      }

      return new SqlFunction(
          name,
          kind,
          Preconditions.checkNotNull(returnTypeInference, "returnTypeInference"),
          new DefaultOperandTypeInference(operandTypes, nullableOperands),
          theOperandTypeChecker,
          functionCategory
      );
    }
  }

  /**
   * Return the default, inferred specific type for a parameter that has a particular type family. Used to infer
   * the type of NULL literals.
   */
  private static SqlTypeName defaultTypeForFamily(final SqlTypeFamily family)
  {
    switch (family) {
      case NUMERIC:
      case APPROXIMATE_NUMERIC:
        return SqlTypeName.DOUBLE;
      case INTEGER:
      case EXACT_NUMERIC:
        return SqlTypeName.BIGINT;
      case CHARACTER:
        return SqlTypeName.VARCHAR;
      case TIMESTAMP:
        return SqlTypeName.TIMESTAMP;
      default:
        // No good default type for this family; just return the first one (or NULL, if empty).
        return Iterables.getFirst(family.getTypeNames(), SqlTypeName.NULL);
    }
  }

  /**
   * Operand type inference that simply reports the types derived by the validator.
   *
   * We do this so that Calcite will allow NULL literals for type-checked operands. Otherwise, it will not be able to
   * infer their types, and it will report them as NULL types, which will make operand type checking fail.
   */
  private static class DefaultOperandTypeInference implements SqlOperandTypeInference
  {
    private final List<SqlTypeFamily> operandTypes;
    private final IntSet nullableOperands;

    DefaultOperandTypeInference(final List<SqlTypeFamily> operandTypes, final IntSet nullableOperands)
    {
      this.operandTypes = operandTypes;
      this.nullableOperands = nullableOperands;
    }

    @Override
    public void inferOperandTypes(
        final SqlCallBinding callBinding,
        final RelDataType returnType,
        final RelDataType[] operandTypesOut
    )
    {
      for (int i = 0; i < operandTypesOut.length; i++) {
        final RelDataType derivedType = callBinding.getValidator()
                                                   .deriveType(callBinding.getScope(), callBinding.operand(i));

        final RelDataType inferredType;

        if (derivedType.getSqlTypeName() != SqlTypeName.NULL) {
          // We could derive a non-NULL type; retain it.
          inferredType = derivedType;
        } else {
          // We couldn't derive a non-NULL type; infer the default for the operand type family.
          if (nullableOperands.contains(i)) {
            inferredType = Calcites.createSqlTypeWithNullability(
                callBinding.getTypeFactory(),
                defaultTypeForFamily(operandTypes.get(i)),
                true
            );
          } else {
            inferredType = callBinding.getValidator().getUnknownType();
          }
        }

        operandTypesOut[i] = inferredType;
      }
    }
  }

  /**
   * Operand type checker that is used in 'simple' situations: there are a particular number of operands, with
   * particular types, some of which may be optional or nullable.
   */
  private static class DefaultOperandTypeChecker implements SqlOperandTypeChecker
  {
    private final List<SqlTypeFamily> operandTypes;
    private final int requiredOperands;
    private final IntSet nullableOperands;

    DefaultOperandTypeChecker(
        final List<SqlTypeFamily> operandTypes,
        final int requiredOperands,
        final IntSet nullableOperands
    )
    {
      Preconditions.checkArgument(requiredOperands <= operandTypes.size() && requiredOperands >= 0);
      this.operandTypes = Preconditions.checkNotNull(operandTypes, "operandTypes");
      this.requiredOperands = requiredOperands;
      this.nullableOperands = Preconditions.checkNotNull(nullableOperands, "nullableOperands");
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure)
    {
      if (operandTypes.size() != callBinding.getOperandCount()) {
        // Just like FamilyOperandTypeChecker: assume this is an inapplicable sub-rule of a composite rule; don't throw
        return false;
      }

      for (int i = 0; i < callBinding.operands().size(); i++) {
        final SqlNode operand = callBinding.operands().get(i);
        final RelDataType operandType = callBinding.getValidator().deriveType(callBinding.getScope(), operand);
        final SqlTypeFamily expectedFamily = operandTypes.get(i);

        if (expectedFamily == SqlTypeFamily.ANY) {
          // ANY matches anything. This operand is all good; do nothing.
        } else if (expectedFamily.getTypeNames().contains(operandType.getSqlTypeName())) {
          // Operand came in with one of the expected types.
        } else if (operandType.getSqlTypeName() == SqlTypeName.NULL) {
          // Null came in, check if operand is a nullable type.
          if (!nullableOperands.contains(i)) {
            if (throwOnFailure) {
              throw callBinding.getValidator().newValidationError(operand, Static.RESOURCE.nullIllegal());
            } else {
              return false;
            }
          }
        } else {
          if (throwOnFailure) {
            throw callBinding.newValidationSignatureError();
          } else {
            return false;
          }
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
  }
}
