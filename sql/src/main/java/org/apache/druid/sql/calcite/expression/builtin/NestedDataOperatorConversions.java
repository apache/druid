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
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.nested.NestedDataComplexTypeSerde;
import org.apache.druid.segment.nested.NestedPathFinder;
import org.apache.druid.segment.nested.NestedPathPart;
import org.apache.druid.segment.virtual.NestedFieldVirtualColumn;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.UnsupportedSQLQueryException;
import org.apache.druid.sql.calcite.planner.convertlet.DruidConvertletFactory;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class NestedDataOperatorConversions
{
  public static final DruidJsonValueConvertletFactory DRUID_JSON_VALUE_CONVERTLET_FACTORY_INSTANCE =
      new DruidJsonValueConvertletFactory();

  public static final SqlReturnTypeInference NESTED_RETURN_TYPE_INFERENCE = opBinding -> RowSignatures.makeComplexType(
      opBinding.getTypeFactory(),
      NestedDataComplexTypeSerde.TYPE,
      true
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
        .operandTypeChecker(
            OperandTypes.sequence(
                "(expr,path)",
                OperandTypes.ANY,
                OperandTypes.and(OperandTypes.family(SqlTypeFamily.STRING), OperandTypes.LITERAL)
            )
        )
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
        .operandTypeChecker(OperandTypes.family(new SqlTypeFamily[]{SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER, SqlTypeFamily.ANY, SqlTypeFamily.ANY, SqlTypeFamily.ANY}))
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

      final Expr pathExpr = Parser.parse(druidExpressions.get(1).getExpression(), plannerContext.getExprMacroTable());
      if (!pathExpr.isLiteral()) {
        return null;
      }
      // pre-normalize path so that the same expressions with different jq syntax are collapsed
      final String path = (String) pathExpr.eval(InputBindings.nilBindings()).value();
      final List<NestedPathPart> parts;
      try {
        parts = NestedPathFinder.parseJsonPath(path);
      }
      catch (IllegalArgumentException iae) {
        throw new UnsupportedSQLQueryException(
            "Cannot use [%s]: [%s]",
            call.getOperator().getName(),
            iae.getMessage()
        );
      }
      final String jsonPath = NestedPathFinder.toNormalizedJsonPath(parts);
      final DruidExpression.ExpressionGenerator builder = (args) ->
          "json_query(" + args.get(0).getExpression() + ",'" + jsonPath + "')";
      if (druidExpressions.get(0).isSimpleExtraction()) {

        return DruidExpression.ofVirtualColumn(
            NestedDataComplexTypeSerde.TYPE,
            builder,
            ImmutableList.of(
                DruidExpression.ofColumn(NestedDataComplexTypeSerde.TYPE, druidExpressions.get(0).getDirectColumn())
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
      return DruidExpression.ofExpression(NestedDataComplexTypeSerde.TYPE, builder, druidExpressions);
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
        // we don't support modifying the behavior to be anything other than 'NULL ON EMPTY' / 'NULL ON ERROR'
        Preconditions.checkArgument(
            "SQLJSONVALUEEMPTYORERRORBEHAVIOR[NULL]".equals(call.operand(2).toString()),
            "Unsupported JSON_VALUE parameter 'ON EMPTY' defined - please re-issue this query without this argument"
        );
        Preconditions.checkArgument(
            "NULL".equals(call.operand(3).toString()),
            "Unsupported JSON_VALUE parameter 'ON EMPTY' defined - please re-issue this query without this argument"
        );
        Preconditions.checkArgument(
            "SQLJSONVALUEEMPTYORERRORBEHAVIOR[NULL]".equals(call.operand(4).toString()),
            "Unsupported JSON_VALUE parameter 'ON ERROR' defined - please re-issue this query without this argument"
        );
        Preconditions.checkArgument(
            "NULL".equals(call.operand(5).toString()),
            "Unsupported JSON_VALUE parameter 'ON ERROR' defined - please re-issue this query without this argument"
        );
        SqlDataTypeSpec dataType = call.operand(6);
        RelDataType sqlType = dataType.deriveType(cx.getValidator());
        SqlNode rewrite;
        if (SqlTypeName.INT_TYPES.contains(sqlType.getSqlTypeName())) {
          rewrite = JsonValueBigintOperatorConversion.FUNCTION.createCall(
              SqlParserPos.ZERO,
              call.operand(0),
              call.operand(1)
          );
        } else if (SqlTypeName.DECIMAL.equals(sqlType.getSqlTypeName()) ||
                   SqlTypeName.APPROX_TYPES.contains(sqlType.getSqlTypeName())) {
          rewrite = JsonValueDoubleOperatorConversion.FUNCTION.createCall(
              SqlParserPos.ZERO,
              call.operand(0),
              call.operand(1)
          );
        } else if (SqlTypeName.STRING_TYPES.contains(sqlType.getSqlTypeName())) {
          rewrite = JsonValueVarcharOperatorConversion.FUNCTION.createCall(
              SqlParserPos.ZERO,
              call.operand(0),
              call.operand(1)
          );
        } else {
          // fallback to json_value_any, e.g. the 'standard' convertlet.
          rewrite = JsonValueAnyOperatorConversion.FUNCTION.createCall(
              SqlParserPos.ZERO,
              call.operand(0),
              call.operand(1)
          );
        }

        // always cast anyway, to prevent haters from complaining that VARCHAR doesn't match VARCHAR(2000)
        SqlNode caster = SqlStdOperatorTable.CAST.createCall(
            SqlParserPos.ZERO,
            rewrite,
            call.operand(6)
        );
        return cx.convertExpression(caster);
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

      final Expr pathExpr = Parser.parse(druidExpressions.get(1).getExpression(), plannerContext.getExprMacroTable());
      if (!pathExpr.isLiteral()) {
        return null;
      }
      // pre-normalize path so that the same expressions with different jq syntax are collapsed
      final String path = (String) pathExpr.eval(InputBindings.nilBindings()).value();
      final List<NestedPathPart> parts;
      try {
        parts = NestedPathFinder.parseJsonPath(path);
      }
      catch (IllegalArgumentException iae) {
        throw new UnsupportedSQLQueryException(
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
                DruidExpression.ofColumn(NestedDataComplexTypeSerde.TYPE, druidExpressions.get(0).getDirectColumn())
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
                                        "(expr,path)",
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

  public static class JsonValueAnyOperatorConversion implements SqlOperatorConversion
  {
    private static final SqlFunction FUNCTION =
        OperatorConversions.operatorBuilder("JSON_VALUE_ANY")
                           .operandTypeChecker(
                               OperandTypes.or(
                                   OperandTypes.sequence(
                                       "(expr,path)",
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

      final Expr pathExpr = Parser.parse(druidExpressions.get(1).getExpression(), plannerContext.getExprMacroTable());
      if (!pathExpr.isLiteral()) {
        return null;
      }
      // pre-normalize path so that the same expressions with different jq syntax are collapsed
      final String path = (String) pathExpr.eval(InputBindings.nilBindings()).value();
      final List<NestedPathPart> parts;
      try {
        parts = NestedPathFinder.parseJsonPath(path);
      }
      catch (IllegalArgumentException iae) {
        throw new UnsupportedSQLQueryException(
            "Cannot use [%s]: [%s]",
            call.getOperator().getName(),
            iae.getMessage()
        );
      }
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
                DruidExpression.ofColumn(NestedDataComplexTypeSerde.TYPE, druidExpressions.get(0).getDirectColumn())
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
              NestedDataComplexTypeSerde.TYPE,
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
              NestedDataComplexTypeSerde.TYPE,
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
              NestedDataComplexTypeSerde.TYPE,
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
              NestedDataComplexTypeSerde.TYPE,
              DruidExpression.functionCall("try_parse_json"),
              druidExpressions
          )
      );
    }
  }
}
