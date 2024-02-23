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

package org.apache.druid.sql.calcite.expression.builtin;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlJsonEmptyOrError;
import org.apache.calcite.sql.SqlJsonValueEmptyOrErrorBehavior;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidSqlInput;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.query.expression.NestedDataExpressions;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.nested.NestedPathFinder;
import org.apache.druid.segment.nested.NestedPathPart;
import org.apache.druid.segment.virtual.NestedFieldVirtualColumn;
import org.apache.druid.sql.calcite.expression.DirectOperatorConversion;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.convertlet.DruidConvertletFactory;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class NestedDataOperatorConversions
{
  public static final DruidJsonValueConvertletFactory DRUID_JSON_VALUE_CONVERTLET_FACTORY_INSTANCE =
      new DruidJsonValueConvertletFactory();

  public static final SqlReturnTypeInference NESTED_RETURN_TYPE_INFERENCE = opBinding -> RowSignatures.makeComplexType(
      opBinding.getTypeFactory(),
      ColumnType.NESTED_DATA,
      true
  );

  public static final SqlReturnTypeInference NESTED_ARRAY_RETURN_TYPE_INFERENCE = opBinding ->
      opBinding.getTypeFactory().createArrayType(
          RowSignatures.makeComplexType(
              opBinding.getTypeFactory(),
              ColumnType.NESTED_DATA,
              true
          ),
          -1
      );

  public static class JsonPathsOperatorConversion implements SqlOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("JSON_PATHS")
        .operandTypeChecker(OperandTypes.ANY)
        .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
        .returnTypeArrayWithNullableElements(SqlTypeName.VARCHAR)
        .build();

    @Override
    public SqlOperator calciteOperator()
    {
      return SQL_FUNCTION;
    }

    @Nullable
    @Override
    public DruidExpression toDruidExpression(
        PlannerContext plannerContext,
        RowSignature rowSignature,
        RexNode rexNode
    )
    {
      return OperatorConversions.convertCall(
          plannerContext,
          rowSignature,
          rexNode,
          druidExpressions -> DruidExpression.ofExpression(
              null,
              DruidExpression.functionCall("json_paths"),
              druidExpressions
          )
      );
    }
  }

  public static class JsonKeysOperatorConversion implements SqlOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("JSON_KEYS")
        .operandNames("expr", "path")
        .operandTypes(SqlTypeFamily.ANY, SqlTypeFamily.STRING)
        .literalOperands(1)
        .requiredOperandCount(2)
        .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
        .returnTypeNullableArrayWithNullableElements(SqlTypeName.VARCHAR)
        .build();

    @Override
    public SqlOperator calciteOperator()
    {
      return SQL_FUNCTION;
    }

    @Nullable
    @Override
    public DruidExpression toDruidExpression(
        PlannerContext plannerContext,
        RowSignature rowSignature,
        RexNode rexNode
    )
    {
      return OperatorConversions.convertCall(
          plannerContext,
          rowSignature,
          rexNode,
          druidExpressions -> DruidExpression.ofExpression(
              ColumnType.STRING_ARRAY,
              DruidExpression.functionCall("json_keys"),
              druidExpressions
          )
      );
    }
  }

  public static class JsonQueryOperatorConversion implements SqlOperatorConversion
  {
    private static final String FUNCTION_NAME = StringUtils.toUpperCase("json_query");
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder(FUNCTION_NAME)
        .operandTypeChecker(
            OperandTypes.family(
                SqlTypeFamily.ANY,
                SqlTypeFamily.CHARACTER,
                SqlTypeFamily.ANY,
                SqlTypeFamily.ANY,
                SqlTypeFamily.ANY
            )
        )
        .returnTypeInference(NESTED_RETURN_TYPE_INFERENCE)
        .functionCategory(SqlFunctionCategory.SYSTEM)
        .build();

    @Override
    public SqlOperator calciteOperator()
    {
      return SQL_FUNCTION;
    }

    @Nullable
    @Override
    public DruidExpression toDruidExpression(
        PlannerContext plannerContext,
        RowSignature rowSignature,
        RexNode rexNode
    )
    {
      final RexCall call = (RexCall) rexNode;

      final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
          plannerContext,
          rowSignature,
          call.getOperands().subList(0, 2)
      );

      if (druidExpressions == null || druidExpressions.size() != 2) {
        return null;
      }

      final Expr pathExpr = plannerContext.parseExpression(druidExpressions.get(1).getExpression());
      if (!pathExpr.isLiteral()) {
        // if path argument is not constant, just use a pure expression
        return DruidExpression.ofFunctionCall(ColumnType.NESTED_DATA, "json_query", druidExpressions);
      }
      // pre-normalize path so that the same expressions with different json path syntax are collapsed
      final String path = (String) pathExpr.eval(InputBindings.nilBindings()).value();
      final List<NestedPathPart> parts = extractNestedPathParts(call, path);
      final String jsonPath = NestedPathFinder.toNormalizedJsonPath(parts);
      final DruidExpression.ExpressionGenerator builder = (args) ->
          "json_query(" + args.get(0).getExpression() + ",'" + jsonPath + "')";
      if (druidExpressions.get(0).isSimpleExtraction()) {

        return DruidExpression.ofVirtualColumn(
            ColumnType.NESTED_DATA,
            builder,
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.NESTED_DATA, druidExpressions.get(0).getDirectColumn())
            ),
            (name, outputType, expression, macroTable) -> new NestedFieldVirtualColumn(
                druidExpressions.get(0).getDirectColumn(),
                name,
                outputType,
                parts,
                true,
                null,
                null
            )
        );
      }
      return DruidExpression.ofExpression(ColumnType.NESTED_DATA, builder, druidExpressions);
    }
  }

  public static class JsonQueryArrayOperatorConversion extends DirectOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder(StringUtils.toUpperCase(NestedDataExpressions.JsonQueryArrayExprMacro.NAME))
        .operandTypeChecker(
            OperandTypes.family(
                SqlTypeFamily.ANY,
                SqlTypeFamily.CHARACTER
            )
        )
        .returnTypeInference(NESTED_ARRAY_RETURN_TYPE_INFERENCE)
        .functionCategory(SqlFunctionCategory.SYSTEM)
        .build();

    public JsonQueryArrayOperatorConversion()
    {
      super(SQL_FUNCTION, NestedDataExpressions.JsonQueryArrayExprMacro.NAME);
    }
  }

  /**
   * The {@link org.apache.calcite.sql2rel.StandardConvertletTable} converts json_value(.. RETURNING type) into
   * cast(json_value_any(..), type).
   *
   * This is not that useful for us, so we have our own convertlet, to translate into specialized operators such
   * as {@link JsonValueBigintOperatorConversion}, {@link JsonValueDoubleOperatorConversion}, or
   * {@link JsonValueVarcharOperatorConversion}, before falling back to {@link JsonValueAnyOperatorConversion}.
   *
   * This convertlet still always wraps the function in a {@link SqlStdOperatorTable#CAST}, to smooth out type
   * mismatches, such as VARCHAR(2000) vs VARCHAR or whatever else various type checkers like to complain about not
   * exactly matching.
   */
  public static class DruidJsonValueConvertletFactory implements DruidConvertletFactory
  {
    @Override
    public SqlRexConvertlet createConvertlet(PlannerContext plannerContext)
    {
      return (cx, call) -> {
        // We don't support modifying the behavior to be anything other than 'NULL ON EMPTY' / 'NULL ON ERROR'.
        // Check this here: prior operand before ON EMPTY or ON ERROR must be NULL.
        for (int i = 2; i < call.operandCount(); i++) {
          final SqlNode operand = call.operand(i);

          if (operand.getKind() == SqlKind.LITERAL
              && ((SqlLiteral) operand).getValue() instanceof SqlJsonEmptyOrError) {
            // Found ON EMPTY or ON ERROR. Check prior operand.
            final SqlNode priorOperand = call.operand(i - 1);
            Preconditions.checkArgument(
                priorOperand.getKind() == SqlKind.LITERAL
                && ((SqlLiteral) priorOperand).getValue() == SqlJsonValueEmptyOrErrorBehavior.NULL,
                "Unsupported JSON_VALUE parameter '%s' defined - please re-issue this query without this argument",
                ((SqlLiteral) operand).getValue()
            );
          }
        }

        RelDataType sqlType = cx.getValidator().getValidatedNodeType(call);
        SqlOperator jsonValueOperator;
        if (SqlTypeName.INT_TYPES.contains(sqlType.getSqlTypeName())) {
          jsonValueOperator = JsonValueBigintOperatorConversion.FUNCTION;
        } else if (SqlTypeName.DECIMAL.equals(sqlType.getSqlTypeName()) ||
                   SqlTypeName.APPROX_TYPES.contains(sqlType.getSqlTypeName())) {
          jsonValueOperator = JsonValueDoubleOperatorConversion.FUNCTION;
        } else if (SqlTypeName.STRING_TYPES.contains(sqlType.getSqlTypeName())) {
          jsonValueOperator = JsonValueVarcharOperatorConversion.FUNCTION;
        } else if (SqlTypeName.ARRAY.equals(sqlType.getSqlTypeName())) {
          ColumnType elementType = Calcites.getColumnTypeForRelDataType(sqlType.getComponentType());
          switch (elementType.getType()) {
            case LONG:
              jsonValueOperator = JsonValueReturningArrayBigIntOperatorConversion.FUNCTION;
              break;
            case DOUBLE:
              jsonValueOperator = JsonValueReturningArrayDoubleOperatorConversion.FUNCTION;
              break;
            case STRING:
              jsonValueOperator = JsonValueReturningArrayVarcharOperatorConversion.FUNCTION;
              break;
            default:
              throw new IAE("Unhandled JSON_VALUE RETURNING ARRAY type [%s]", sqlType.getComponentType());
          }
        } else {
          // fallback to json_value_any, e.g. the 'standard' convertlet.
          jsonValueOperator = JsonValueAnyOperatorConversion.FUNCTION;
        }


        // always cast anyway, to prevent haters from complaining that VARCHAR doesn't match VARCHAR(2000)
        return cx.getRexBuilder().makeCast(
            sqlType,
            cx.getRexBuilder().makeCall(
                jsonValueOperator,
                cx.convertExpression(call.operand(0)),
                cx.convertExpression(call.operand(1))
            )
        );
      };
    }

    @Override
    public List<SqlOperator> operators()
    {
      return Collections.singletonList(SqlStdOperatorTable.JSON_VALUE);
    }
  }

  public abstract static class JsonValueReturningTypeOperatorConversion implements SqlOperatorConversion
  {
    private final SqlFunction function;
    private final ColumnType druidType;

    public JsonValueReturningTypeOperatorConversion(SqlFunction function, ColumnType druidType)
    {
      this.druidType = druidType;
      this.function = function;
    }

    @Override
    public SqlOperator calciteOperator()
    {
      return function;
    }

    @Nullable
    @Override
    public DruidExpression toDruidExpression(
        PlannerContext plannerContext,
        RowSignature rowSignature,
        RexNode rexNode
    )
    {
      final RexCall call = (RexCall) rexNode;
      final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
          plannerContext,
          rowSignature,
          call.getOperands()
      );

      if (druidExpressions == null || druidExpressions.size() != 2) {
        return null;
      }

      final Expr pathExpr = plannerContext.parseExpression(druidExpressions.get(1).getExpression());
      if (!pathExpr.isLiteral()) {
        // if path argument is not constant, just use a pure expression
        return DruidExpression.ofFunctionCall(
            druidType,
            "json_value",
            ImmutableList.<DruidExpression>builder()
                         .addAll(druidExpressions)
                         .add(DruidExpression.ofStringLiteral(druidType.asTypeString()))
                         .build()
        );
      }
      // pre-normalize path so that the same expressions with different json path syntax are collapsed
      final String path = (String) pathExpr.eval(InputBindings.nilBindings()).value();

      final List<NestedPathPart> parts = extractNestedPathParts(call, path);

      final String jsonPath = NestedPathFinder.toNormalizedJsonPath(parts);
      final DruidExpression.ExpressionGenerator builder = (args) ->
          "json_value(" + args.get(0).getExpression() + ",'" + jsonPath + "', '" + druidType.asTypeString() + "')";

      if (druidExpressions.get(0).isSimpleExtraction()) {

        return DruidExpression.ofVirtualColumn(
            druidType,
            builder,
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.NESTED_DATA, druidExpressions.get(0).getDirectColumn())
            ),
            (name, outputType, expression, macroTable) -> new NestedFieldVirtualColumn(
                druidExpressions.get(0).getDirectColumn(),
                name,
                outputType,
                parts,
                false,
                null,
                null
            )
        );
      }
      return DruidExpression.ofExpression(druidType, builder, druidExpressions);
    }

    static SqlFunction buildFunction(String functionName, SqlTypeName typeName)
    {
      return OperatorConversions.operatorBuilder(functionName)
                                .operandTypeChecker(
                                    OperandTypes.sequence(
                                        "'" + functionName + "(expr, path)'",
                                        OperandTypes.family(SqlTypeFamily.ANY),
                                        OperandTypes.family(SqlTypeFamily.STRING)
                                    )
                                )
                                .returnTypeInference(
                                    ReturnTypes.cascade(
                                        opBinding -> opBinding.getTypeFactory().createSqlType(typeName),
                                        SqlTypeTransforms.FORCE_NULLABLE
                                    )
                                )
                                .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
                                .build();
    }
  }

  public static class JsonValueBigintOperatorConversion extends JsonValueReturningTypeOperatorConversion
  {
    private static final SqlFunction FUNCTION = buildFunction("JSON_VALUE_BIGINT", SqlTypeName.BIGINT);

    public JsonValueBigintOperatorConversion()
    {
      super(FUNCTION, ColumnType.LONG);
    }
  }

  public static class JsonValueDoubleOperatorConversion extends JsonValueReturningTypeOperatorConversion
  {
    private static final SqlFunction FUNCTION = buildFunction("JSON_VALUE_DOUBLE", SqlTypeName.DOUBLE);

    public JsonValueDoubleOperatorConversion()
    {
      super(FUNCTION, ColumnType.DOUBLE);
    }
  }

  public static class JsonValueVarcharOperatorConversion extends JsonValueReturningTypeOperatorConversion
  {
    private static final SqlFunction FUNCTION = buildFunction("JSON_VALUE_VARCHAR", SqlTypeName.VARCHAR);

    public JsonValueVarcharOperatorConversion()
    {
      super(FUNCTION, ColumnType.STRING);
    }
  }

  public abstract static class JsonValueReturningArrayTypeOperatorConversion implements SqlOperatorConversion
  {
    private final SqlFunction function;
    private final ColumnType druidType;

    public JsonValueReturningArrayTypeOperatorConversion(SqlFunction function, ColumnType druidType)
    {
      this.druidType = druidType;
      this.function = function;
    }

    @Override
    public SqlOperator calciteOperator()
    {
      return function;
    }

    @Nullable
    @Override
    public DruidExpression toDruidExpression(
        PlannerContext plannerContext,
        RowSignature rowSignature,
        RexNode rexNode
    )
    {
      final RexCall call = (RexCall) rexNode;
      final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
          plannerContext,
          rowSignature,
          call.getOperands()
      );

      if (druidExpressions == null || druidExpressions.size() != 2) {
        return null;
      }

      final Expr pathExpr = plannerContext.parseExpression(druidExpressions.get(1).getExpression());
      if (!pathExpr.isLiteral()) {
        return null;
      }
      // pre-normalize path so that the same expressions with different json path syntax are collapsed
      final String path = (String) pathExpr.eval(InputBindings.nilBindings()).value();
      final List<NestedPathPart> parts;
      try {
        parts = NestedPathFinder.parseJsonPath(path);
      }
      catch (IllegalArgumentException iae) {
        throw InvalidSqlInput.exception(
            "Cannot use [%s]: [%s]",
            call.getOperator().getName(),
            iae.getMessage()
        );
      }
      final String jsonPath = NestedPathFinder.toNormalizedJsonPath(parts);
      final DruidExpression.ExpressionGenerator builder = (args) ->
          "json_value(" + args.get(0).getExpression() + ",'" + jsonPath + "', '" + druidType.asTypeString() + "')";

      if (druidExpressions.get(0).isSimpleExtraction()) {

        return DruidExpression.ofVirtualColumn(
            druidType,
            builder,
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.NESTED_DATA, druidExpressions.get(0).getDirectColumn())
            ),
            (name, outputType, expression, macroTable) -> new NestedFieldVirtualColumn(
                druidExpressions.get(0).getDirectColumn(),
                name,
                outputType,
                parts,
                false,
                null,
                null
            )
        );
      }
      return DruidExpression.ofExpression(druidType, builder, druidExpressions);
    }

    static SqlFunction buildArrayFunction(String functionName, SqlTypeName elementTypeName)
    {
      return OperatorConversions.operatorBuilder(functionName)
                                .operandTypeChecker(
                                    OperandTypes.sequence(
                                        "'" + functionName + "(expr, path)'",
                                        OperandTypes.family(SqlTypeFamily.ANY),
                                        OperandTypes.family(SqlTypeFamily.STRING)
                                    )
                                )
                                .returnTypeInference(
                                    opBinding -> {
                                      return opBinding.getTypeFactory().createTypeWithNullability(Calcites.createSqlArrayTypeWithNullability(opBinding.getTypeFactory(), elementTypeName, false), true);
                                    }
                                )
                                .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
                                .build();
    }
  }

  public static class JsonValueReturningArrayBigIntOperatorConversion extends JsonValueReturningArrayTypeOperatorConversion
  {
    static final SqlFunction FUNCTION = buildArrayFunction("JSON_VALUE_ARRAY_BIGINT", SqlTypeName.BIGINT);

    public JsonValueReturningArrayBigIntOperatorConversion()
    {
      super(FUNCTION, ColumnType.LONG_ARRAY);
    }
  }

  public static class JsonValueReturningArrayDoubleOperatorConversion extends JsonValueReturningArrayTypeOperatorConversion
  {
    static final SqlFunction FUNCTION = buildArrayFunction("JSON_VALUE_ARRAY_DOUBLE", SqlTypeName.DOUBLE);

    public JsonValueReturningArrayDoubleOperatorConversion()
    {
      super(FUNCTION, ColumnType.DOUBLE_ARRAY);
    }
  }

  public static class JsonValueReturningArrayVarcharOperatorConversion extends JsonValueReturningArrayTypeOperatorConversion
  {
    static final SqlFunction FUNCTION = buildArrayFunction("JSON_VALUE_ARRAY_VARCHAR", SqlTypeName.VARCHAR);

    public JsonValueReturningArrayVarcharOperatorConversion()
    {
      super(FUNCTION, ColumnType.STRING_ARRAY);
    }
  }

  public static class JsonValueAnyOperatorConversion implements SqlOperatorConversion
  {
    private static final SqlFunction FUNCTION =
        OperatorConversions.operatorBuilder("JSON_VALUE_ANY")
                           .operandTypeChecker(
                               OperandTypes.or(
                                   OperandTypes.sequence(
                                       "'JSON_VALUE_ANY(expr, path)'",
                                       OperandTypes.family(SqlTypeFamily.ANY),
                                       OperandTypes.family(SqlTypeFamily.STRING)
                                   ),
                                   OperandTypes.family(
                                       SqlTypeFamily.ANY,
                                       SqlTypeFamily.CHARACTER,
                                       SqlTypeFamily.ANY,
                                       SqlTypeFamily.ANY,
                                       SqlTypeFamily.ANY,
                                       SqlTypeFamily.ANY,
                                       SqlTypeFamily.ANY
                                   )
                               )
                           )
                           .operandTypeInference((callBinding, returnType, operandTypes) -> {
                             RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
                             if (operandTypes.length > 5) {
                               operandTypes[3] = typeFactory.createSqlType(SqlTypeName.ANY);
                               operandTypes[5] = typeFactory.createSqlType(SqlTypeName.ANY);
                             }
                           })
                           .returnTypeInference(
                               ReturnTypes.cascade(
                                   opBinding -> opBinding.getTypeFactory().createTypeWithNullability(
                                       // STRING is the closest thing we have to an ANY type
                                       // however, this should really be using SqlTypeName.ANY.. someday
                                       opBinding.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),
                                       true
                                   ),
                                   SqlTypeTransforms.FORCE_NULLABLE
                               )
                           )
                           .functionCategory(SqlFunctionCategory.SYSTEM)
                           .build();

    @Override
    public SqlOperator calciteOperator()
    {
      return FUNCTION;
    }

    @Nullable
    @Override
    public DruidExpression toDruidExpression(
        PlannerContext plannerContext,
        RowSignature rowSignature,
        RexNode rexNode
    )
    {
      final RexCall call = (RexCall) rexNode;

      // calcite parser can allow for a bunch of junk in here that we don't care about right now, so the call looks
      // something like this:
      // JSON_VALUE_ANY(`nested`.`nest`, '$.x', SQLJSONVALUEEMPTYORERRORBEHAVIOR[NULL], NULL, SQLJSONVALUEEMPTYORERRORBEHAVIOR[NULL], NULL)
      // by the time it gets here

      final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
          plannerContext,
          rowSignature,
          call.getOperands().size() > 2 ? call.getOperands().subList(0, 2) : call.getOperands()
      );


      if (druidExpressions == null || druidExpressions.size() != 2) {
        return null;
      }

      final Expr pathExpr = plannerContext.parseExpression(druidExpressions.get(1).getExpression());
      if (!pathExpr.isLiteral()) {
        return null;
      }
      // pre-normalize path so that the same expressions with different json path syntax are collapsed
      final String path = (String) pathExpr.eval(InputBindings.nilBindings()).value();
      final List<NestedPathPart> parts = extractNestedPathParts(call, path);
      final String jsonPath = NestedPathFinder.toNormalizedJsonPath(parts);
      final DruidExpression.ExpressionGenerator builder = (args) ->
          "json_value(" + args.get(0).getExpression() + ",'" + jsonPath + "')";

      // STRING is the closest thing we have to ANY, though maybe someday this
      // can be replaced with a VARIANT type
      final ColumnType columnType = ColumnType.STRING;

      if (druidExpressions.get(0).isSimpleExtraction()) {
        return DruidExpression.ofVirtualColumn(
            columnType,
            builder,
            ImmutableList.of(
                DruidExpression.ofColumn(ColumnType.NESTED_DATA, druidExpressions.get(0).getDirectColumn())
            ),
            (name, outputType, expression, macroTable) -> new NestedFieldVirtualColumn(
                druidExpressions.get(0).getDirectColumn(),
                name,
                null,
                parts,
                false,
                null,
                null
            )
        );
      }
      return DruidExpression.ofExpression(columnType, builder, druidExpressions);
    }
  }

  public static class JsonObjectOperatorConversion implements SqlOperatorConversion
  {
    private static final String FUNCTION_NAME = "json_object";
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder(FUNCTION_NAME)
        .operandTypeChecker(OperandTypes.variadic(SqlOperandCountRanges.from(1)))
        .operandTypeInference((callBinding, returnType, operandTypes) -> {
          RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
          for (int i = 0; i < operandTypes.length; i++) {
            if (i % 2 == 0) {
              operandTypes[i] = typeFactory.createSqlType(SqlTypeName.VARCHAR);
              continue;
            }
            operandTypes[i] = typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.ANY),
                true
            );
          }
        })
        .returnTypeInference(NESTED_RETURN_TYPE_INFERENCE)
        .functionCategory(SqlFunctionCategory.SYSTEM)
        .build();

    @Override
    public SqlOperator calciteOperator()
    {
      return SQL_FUNCTION;
    }

    @Nullable
    @Override
    public DruidExpression toDruidExpression(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode)
    {
      final DruidExpression.DruidExpressionCreator expressionFunction = druidExpressions ->
          DruidExpression.ofExpression(
              ColumnType.NESTED_DATA,
              null,
              DruidExpression.functionCall("json_object"),
              druidExpressions
          );

      final RexCall call = (RexCall) rexNode;

      // we ignore the first argument because calcite sets a 'nullBehavior' parameter by the time it gets here
      // that we .. dont care about right now
      final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
          plannerContext,
          rowSignature,
          call.getOperands().subList(1, call.getOperands().size())
      );

      if (druidExpressions == null) {
        return null;
      }

      return expressionFunction.create(druidExpressions);
    }
  }

  public static class ToJsonStringOperatorConversion implements SqlOperatorConversion
  {
    private static final String FUNCTION_NAME = "to_json_string";
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder(StringUtils.toUpperCase(FUNCTION_NAME))
        .operandTypes(SqlTypeFamily.ANY)
        .returnTypeCascadeNullable(SqlTypeName.VARCHAR)
        .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
        .build();


    @Override
    public SqlOperator calciteOperator()
    {
      return SQL_FUNCTION;
    }

    @Nullable
    @Override
    public DruidExpression toDruidExpression(
        PlannerContext plannerContext,
        RowSignature rowSignature,
        RexNode rexNode
    )
    {
      return OperatorConversions.convertCall(
          plannerContext,
          rowSignature,
          rexNode,
          druidExpressions -> DruidExpression.ofExpression(
              ColumnType.NESTED_DATA,
              DruidExpression.functionCall("to_json_string"),
              druidExpressions
          )
      );
    }
  }

  public static class ParseJsonOperatorConversion implements SqlOperatorConversion
  {
    private static final String FUNCTION_NAME = "parse_json";
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder(StringUtils.toUpperCase(FUNCTION_NAME))
        .operandTypes(SqlTypeFamily.STRING)
        .returnTypeInference(NESTED_RETURN_TYPE_INFERENCE)
        .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
        .build();


    @Override
    public SqlOperator calciteOperator()
    {
      return SQL_FUNCTION;
    }

    @Nullable
    @Override
    public DruidExpression toDruidExpression(
        PlannerContext plannerContext,
        RowSignature rowSignature,
        RexNode rexNode
    )
    {
      return OperatorConversions.convertCall(
          plannerContext,
          rowSignature,
          rexNode,
          druidExpressions -> DruidExpression.ofExpression(
              ColumnType.NESTED_DATA,
              DruidExpression.functionCall("parse_json"),
              druidExpressions
          )
      );
    }
  }

  public static class TryParseJsonOperatorConversion implements SqlOperatorConversion
  {
    private static final String FUNCTION_NAME = "try_parse_json";
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder(StringUtils.toUpperCase(FUNCTION_NAME))
        .operandTypes(SqlTypeFamily.STRING)
        .returnTypeInference(NESTED_RETURN_TYPE_INFERENCE)
        .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
        .build();


    @Override
    public SqlOperator calciteOperator()
    {
      return SQL_FUNCTION;
    }

    @Nullable
    @Override
    public DruidExpression toDruidExpression(
        PlannerContext plannerContext,
        RowSignature rowSignature,
        RexNode rexNode
    )
    {
      return OperatorConversions.convertCall(
          plannerContext,
          rowSignature,
          rexNode,
          druidExpressions -> DruidExpression.ofExpression(
              ColumnType.NESTED_DATA,
              DruidExpression.functionCall("try_parse_json"),
              druidExpressions
          )
      );
    }
  }

  @Nonnull
  private static List<NestedPathPart> extractNestedPathParts(RexCall call, String path)
  {
    try {
      return NestedPathFinder.parseJsonPath(path);
    }
    catch (IllegalArgumentException iae) {
      final String name = call.getOperator().getName();
      throw DruidException
          .forPersona(DruidException.Persona.USER)
          .ofCategory(DruidException.Category.INVALID_INPUT)
          .build(iae, "Error when processing path [%s], operator [%s] is not useable", path, name);
    }
  }
}
