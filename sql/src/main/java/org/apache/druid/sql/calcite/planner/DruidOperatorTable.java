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

package org.apache.druid.sql.calcite.planner;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.aggregation.builtin.ApproxCountDistinctSqlAggregator;
import org.apache.druid.sql.calcite.aggregation.builtin.AvgSqlAggregator;
import org.apache.druid.sql.calcite.aggregation.builtin.CountSqlAggregator;
import org.apache.druid.sql.calcite.aggregation.builtin.MaxSqlAggregator;
import org.apache.druid.sql.calcite.aggregation.builtin.MinSqlAggregator;
import org.apache.druid.sql.calcite.aggregation.builtin.SumSqlAggregator;
import org.apache.druid.sql.calcite.aggregation.builtin.SumZeroSqlAggregator;
import org.apache.druid.sql.calcite.expression.AliasedOperatorConversion;
import org.apache.druid.sql.calcite.expression.BinaryOperatorConversion;
import org.apache.druid.sql.calcite.expression.DirectOperatorConversion;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.expression.UnaryFunctionOperatorConversion;
import org.apache.druid.sql.calcite.expression.UnaryPrefixOperatorConversion;
import org.apache.druid.sql.calcite.expression.UnarySuffixOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.ArrayConstructorOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.ArrayContainsOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.ArrayLengthOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.ArrayOffsetOfOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.ArrayOffsetOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.ArrayOrdinalOfOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.ArrayOrdinalOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.ArrayOverlapOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.ArrayToStringOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.BTrimOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.CastOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.CeilOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.ConcatOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.DateTruncOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.ExtractOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.FloorOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.IPv4AddressMatchOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.IPv4AddressParseOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.IPv4AddressStringifyOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.LPadOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.LTrimOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.LeftOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.LikeOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.MillisToTimestampOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.MultiValueStringAppendOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.MultiValueStringConcatOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.MultiValueStringPrependOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.MultiValueStringSliceOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.ParseLongOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.PositionOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.RPadOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.RTrimOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.RegexpExtractOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.ReinterpretOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.RepeatOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.ReverseOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.RightOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.RoundOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.StringFormatOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.StringToMultiValueStringOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.StrposOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.SubstringOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TextcatOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimeArithmeticOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimeCeilOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimeExtractOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimeFloorOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimeFormatOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimeParseOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimeShiftOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TimestampToMillisOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TrimOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.TruncateOperatorConversion;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DruidOperatorTable implements SqlOperatorTable
{
  private static final List<SqlAggregator> STANDARD_AGGREGATORS =
      ImmutableList.<SqlAggregator>builder()
          .add(new ApproxCountDistinctSqlAggregator())
          .add(new AvgSqlAggregator())
          .add(new CountSqlAggregator())
          .add(new MinSqlAggregator())
          .add(new MaxSqlAggregator())
          .add(new SumSqlAggregator())
          .add(new SumZeroSqlAggregator())
          .build();


  // STRLEN has so many aliases.
  private static final SqlOperatorConversion CHARACTER_LENGTH_CONVERSION = new DirectOperatorConversion(
      SqlStdOperatorTable.CHARACTER_LENGTH,
      "strlen"
  );

  private static final List<SqlOperatorConversion> TIME_OPERATOR_CONVERSIONS =
      ImmutableList.<SqlOperatorConversion>builder()
          .add(new CeilOperatorConversion())
          .add(new DateTruncOperatorConversion())
          .add(new ExtractOperatorConversion())
          .add(new FloorOperatorConversion())
          .add(new MillisToTimestampOperatorConversion())
          .add(new TimeArithmeticOperatorConversion.TimeMinusIntervalOperatorConversion())
          .add(new TimeArithmeticOperatorConversion.TimePlusIntervalOperatorConversion())
          .add(new TimeExtractOperatorConversion())
          .add(new TimeCeilOperatorConversion())
          .add(new TimeFloorOperatorConversion())
          .add(new TimeFormatOperatorConversion())
          .add(new TimeParseOperatorConversion())
          .add(new TimeShiftOperatorConversion())
          .add(new TimestampToMillisOperatorConversion())
          .build();

  private static final List<SqlOperatorConversion> STRING_OPERATOR_CONVERSIONS =
      ImmutableList.<SqlOperatorConversion>builder()
          .add(new BTrimOperatorConversion())
          .add(new LikeOperatorConversion())
          .add(new LTrimOperatorConversion())
          .add(new PositionOperatorConversion())
          .add(new RegexpExtractOperatorConversion())
          .add(new RTrimOperatorConversion())
          .add(new ParseLongOperatorConversion())
          .add(new StringFormatOperatorConversion())
          .add(new StrposOperatorConversion())
          .add(new SubstringOperatorConversion())
          .add(new RightOperatorConversion())
          .add(new LeftOperatorConversion())
          .add(new ReverseOperatorConversion())
          .add(new RepeatOperatorConversion())
          .add(new AliasedOperatorConversion(new SubstringOperatorConversion(), "SUBSTR"))
          .add(new ConcatOperatorConversion())
          .add(new TextcatOperatorConversion())
          .add(new TrimOperatorConversion())
          .add(new TruncateOperatorConversion())
          .add(new AliasedOperatorConversion(new TruncateOperatorConversion(), "TRUNC"))
          .add(new LPadOperatorConversion())
          .add(new RPadOperatorConversion())
          .build();

  private static final List<SqlOperatorConversion> VALUE_COERCION_OPERATOR_CONVERSIONS =
      ImmutableList.<SqlOperatorConversion>builder()
          .add(new CastOperatorConversion())
          .add(new ReinterpretOperatorConversion())
          .build();

  private static final List<SqlOperatorConversion> ARRAY_OPERATOR_CONVERSIONS =
      ImmutableList.<SqlOperatorConversion>builder()
          .add(new ArrayConstructorOperatorConversion())
          .add(new ArrayContainsOperatorConversion())
          .add(new ArrayOverlapOperatorConversion())
          .add(new AliasedOperatorConversion(new ArrayContainsOperatorConversion(), "MV_CONTAINS"))
          .add(new AliasedOperatorConversion(new ArrayOverlapOperatorConversion(), "MV_OVERLAP"))
          .add(new ArrayLengthOperatorConversion())
          .add(new AliasedOperatorConversion(new ArrayLengthOperatorConversion(), "MV_LENGTH"))
          .add(new ArrayOffsetOperatorConversion())
          .add(new AliasedOperatorConversion(new ArrayOffsetOperatorConversion(), "MV_OFFSET"))
          .add(new ArrayOrdinalOperatorConversion())
          .add(new AliasedOperatorConversion(new ArrayOrdinalOperatorConversion(), "MV_ORDINAL"))
          .add(new ArrayOffsetOfOperatorConversion())
          .add(new AliasedOperatorConversion(new ArrayOffsetOfOperatorConversion(), "MV_OFFSET_OF"))
          .add(new ArrayOrdinalOfOperatorConversion())
          .add(new AliasedOperatorConversion(new ArrayOrdinalOfOperatorConversion(), "MV_ORDINAL_OF"))
          .add(new ArrayToStringOperatorConversion())
          .add(new AliasedOperatorConversion(new ArrayToStringOperatorConversion(), "MV_TO_STRING"))
          .build();

  private static final List<SqlOperatorConversion> MULTIVALUE_STRING_OPERATOR_CONVERSIONS =
      ImmutableList.<SqlOperatorConversion>builder()
          .add(new MultiValueStringAppendOperatorConversion())
          .add(new MultiValueStringConcatOperatorConversion())
          .add(new MultiValueStringPrependOperatorConversion())
          .add(new MultiValueStringSliceOperatorConversion())
          .add(new StringToMultiValueStringOperatorConversion())
          .build();

  private static final List<SqlOperatorConversion> IPV4ADDRESS_OPERATOR_CONVERSIONS =
      ImmutableList.<SqlOperatorConversion>builder()
          .add(new IPv4AddressMatchOperatorConversion())
          .add(new IPv4AddressParseOperatorConversion())
          .add(new IPv4AddressStringifyOperatorConversion())
          .build();

  private static final List<SqlOperatorConversion> STANDARD_OPERATOR_CONVERSIONS =
      ImmutableList.<SqlOperatorConversion>builder()
          .add(new DirectOperatorConversion(SqlStdOperatorTable.ABS, "abs"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.CASE, "case_searched"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.CHAR_LENGTH, "strlen"))
          .add(CHARACTER_LENGTH_CONVERSION)
          .add(new AliasedOperatorConversion(CHARACTER_LENGTH_CONVERSION, "LENGTH"))
          .add(new AliasedOperatorConversion(CHARACTER_LENGTH_CONVERSION, "STRLEN"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.CONCAT, "concat"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.EXP, "exp"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.DIVIDE_INTEGER, "div"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.LN, "log"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.LOWER, "lower"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.LOG10, "log10"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.POWER, "pow"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.REPLACE, "replace"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.SQRT, "sqrt"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.UPPER, "upper"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.PI, "pi"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.SIN, "sin"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.COS, "cos"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.TAN, "tan"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.COT, "cot"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.ASIN, "asin"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.ACOS, "acos"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.ATAN, "atan"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.ATAN2, "atan2"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.RADIANS, "toRadians"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.DEGREES, "toDegrees"))
          .add(new UnaryPrefixOperatorConversion(SqlStdOperatorTable.NOT, "!"))
          .add(new UnaryPrefixOperatorConversion(SqlStdOperatorTable.UNARY_MINUS, "-"))
          .add(new UnaryFunctionOperatorConversion(SqlStdOperatorTable.IS_NULL, "isnull"))
          .add(new UnaryFunctionOperatorConversion(SqlStdOperatorTable.IS_NOT_NULL, "notnull"))
          .add(new UnarySuffixOperatorConversion(SqlStdOperatorTable.IS_FALSE, "<= 0")) // Matches Evals.asBoolean
          .add(new UnarySuffixOperatorConversion(SqlStdOperatorTable.IS_NOT_TRUE, "<= 0")) // Matches Evals.asBoolean
          .add(new UnarySuffixOperatorConversion(SqlStdOperatorTable.IS_TRUE, "> 0")) // Matches Evals.asBoolean
          .add(new UnarySuffixOperatorConversion(SqlStdOperatorTable.IS_NOT_FALSE, "> 0")) // Matches Evals.asBoolean
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.MULTIPLY, "*"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.MOD, "%"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.DIVIDE, "/"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.PLUS, "+"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.MINUS, "-"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.EQUALS, "=="))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.NOT_EQUALS, "!="))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.GREATER_THAN, ">"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, ">="))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.LESS_THAN, "<"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, "<="))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.AND, "&&"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.OR, "||"))
          .add(new RoundOperatorConversion())
          .addAll(TIME_OPERATOR_CONVERSIONS)
          .addAll(STRING_OPERATOR_CONVERSIONS)
          .addAll(VALUE_COERCION_OPERATOR_CONVERSIONS)
          .addAll(ARRAY_OPERATOR_CONVERSIONS)
          .addAll(MULTIVALUE_STRING_OPERATOR_CONVERSIONS)
          .addAll(IPV4ADDRESS_OPERATOR_CONVERSIONS)
          .build();

  // Operators that have no conversion, but are handled in the convertlet table, so they still need to exist.
  private static final Map<OperatorKey, SqlOperator> CONVERTLET_OPERATORS =
      DruidConvertletTable.knownOperators()
                          .stream()
                          .collect(Collectors.toMap(OperatorKey::of, Function.identity()));

  private final Map<OperatorKey, SqlAggregator> aggregators;
  private final Map<OperatorKey, SqlOperatorConversion> operatorConversions;

  @Inject
  public DruidOperatorTable(
      final Set<SqlAggregator> aggregators,
      final Set<SqlOperatorConversion> operatorConversions
  )
  {
    this.aggregators = new HashMap<>();
    this.operatorConversions = new HashMap<>();

    for (SqlAggregator aggregator : aggregators) {
      final OperatorKey operatorKey = OperatorKey.of(aggregator.calciteFunction());
      if (this.aggregators.put(operatorKey, aggregator) != null) {
        throw new ISE("Cannot have two operators with key[%s]", operatorKey);
      }
    }

    for (SqlAggregator aggregator : STANDARD_AGGREGATORS) {
      final OperatorKey operatorKey = OperatorKey.of(aggregator.calciteFunction());

      // Don't complain if the name already exists; we allow standard operators to be overridden.
      this.aggregators.putIfAbsent(operatorKey, aggregator);
    }

    for (SqlOperatorConversion operatorConversion : operatorConversions) {
      final OperatorKey operatorKey = OperatorKey.of(operatorConversion.calciteOperator());
      if (this.aggregators.containsKey(operatorKey)
          || this.operatorConversions.put(operatorKey, operatorConversion) != null) {
        throw new ISE("Cannot have two operators with key[%s]", operatorKey);
      }
    }

    for (SqlOperatorConversion operatorConversion : STANDARD_OPERATOR_CONVERSIONS) {
      final OperatorKey operatorKey = OperatorKey.of(operatorConversion.calciteOperator());

      // Don't complain if the name already exists; we allow standard operators to be overridden.
      if (this.aggregators.containsKey(operatorKey)) {
        continue;
      }

      this.operatorConversions.putIfAbsent(operatorKey, operatorConversion);
    }
  }

  @Nullable
  public SqlAggregator lookupAggregator(final SqlAggFunction aggFunction)
  {
    final SqlAggregator sqlAggregator = aggregators.get(OperatorKey.of(aggFunction));
    if (sqlAggregator != null && sqlAggregator.calciteFunction().equals(aggFunction)) {
      return sqlAggregator;
    } else {
      return null;
    }
  }

  @Nullable
  public SqlOperatorConversion lookupOperatorConversion(final SqlOperator operator)
  {
    final SqlOperatorConversion operatorConversion = operatorConversions.get(OperatorKey.of(operator));
    if (operatorConversion != null && operatorConversion.calciteOperator().equals(operator)) {
      return operatorConversion;
    } else {
      return null;
    }
  }

  @Override
  public void lookupOperatorOverloads(
      final SqlIdentifier opName,
      final SqlFunctionCategory category,
      final SqlSyntax syntax,
      final List<SqlOperator> operatorList
  )
  {
    if (opName == null) {
      return;
    }

    if (opName.names.size() != 1) {
      return;
    }

    final OperatorKey operatorKey = OperatorKey.of(opName.getSimple(), syntax);

    final SqlAggregator aggregator = aggregators.get(operatorKey);
    if (aggregator != null) {
      operatorList.add(aggregator.calciteFunction());
    }

    final SqlOperatorConversion operatorConversion = operatorConversions.get(operatorKey);
    if (operatorConversion != null) {
      operatorList.add(operatorConversion.calciteOperator());
    }

    final SqlOperator convertletOperator = CONVERTLET_OPERATORS.get(operatorKey);
    if (convertletOperator != null) {
      operatorList.add(convertletOperator);
    }
  }

  @Override
  public List<SqlOperator> getOperatorList()
  {
    final List<SqlOperator> retVal = new ArrayList<>();
    for (SqlAggregator aggregator : aggregators.values()) {
      retVal.add(aggregator.calciteFunction());
    }
    for (SqlOperatorConversion operatorConversion : operatorConversions.values()) {
      retVal.add(operatorConversion.calciteOperator());
    }
    retVal.addAll(DruidConvertletTable.knownOperators());
    return retVal;
  }

  private static SqlSyntax normalizeSyntax(final SqlSyntax syntax)
  {
    // Treat anything other than prefix/suffix/binary syntax as function syntax.
    if (syntax == SqlSyntax.PREFIX || syntax == SqlSyntax.BINARY || syntax == SqlSyntax.POSTFIX) {
      return syntax;
    } else {
      return SqlSyntax.FUNCTION;
    }
  }

  private static class OperatorKey
  {
    private final String name;
    private final SqlSyntax syntax;

    OperatorKey(final String name, final SqlSyntax syntax)
    {
      this.name = StringUtils.toLowerCase(Preconditions.checkNotNull(name, "name"));
      this.syntax = normalizeSyntax(Preconditions.checkNotNull(syntax, "syntax"));
    }

    public static OperatorKey of(final String name, final SqlSyntax syntax)
    {
      return new OperatorKey(name, syntax);
    }

    public static OperatorKey of(final SqlOperator operator)
    {
      return new OperatorKey(operator.getName(), operator.getSyntax());
    }

    public String getName()
    {
      return name;
    }

    public SqlSyntax getSyntax()
    {
      return syntax;
    }

    @Override
    public boolean equals(final Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final OperatorKey that = (OperatorKey) o;
      return Objects.equals(name, that.name) &&
             syntax == that.syntax;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(name, syntax);
    }

    @Override
    public String toString()
    {
      return "OperatorKey{" +
             "name='" + name + '\'' +
             ", syntax=" + syntax +
             '}';
    }
  }
}
