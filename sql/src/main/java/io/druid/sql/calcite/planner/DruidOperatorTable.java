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

package io.druid.sql.calcite.planner;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.sql.calcite.aggregation.SqlAggregator;
import io.druid.sql.calcite.aggregation.builtin.ApproxCountDistinctSqlAggregator;
import io.druid.sql.calcite.aggregation.builtin.AvgSqlAggregator;
import io.druid.sql.calcite.aggregation.builtin.CountSqlAggregator;
import io.druid.sql.calcite.aggregation.builtin.MaxSqlAggregator;
import io.druid.sql.calcite.aggregation.builtin.MinSqlAggregator;
import io.druid.sql.calcite.aggregation.builtin.SumSqlAggregator;
import io.druid.sql.calcite.aggregation.builtin.SumZeroSqlAggregator;
import io.druid.sql.calcite.expression.AliasedOperatorConversion;
import io.druid.sql.calcite.expression.BinaryOperatorConversion;
import io.druid.sql.calcite.expression.DirectOperatorConversion;
import io.druid.sql.calcite.expression.SqlOperatorConversion;
import io.druid.sql.calcite.expression.UnaryPrefixOperatorConversion;
import io.druid.sql.calcite.expression.UnarySuffixOperatorConversion;
import io.druid.sql.calcite.expression.builtin.BTrimOperatorConversion;
import io.druid.sql.calcite.expression.builtin.CastOperatorConversion;
import io.druid.sql.calcite.expression.builtin.CeilOperatorConversion;
import io.druid.sql.calcite.expression.builtin.DateTruncOperatorConversion;
import io.druid.sql.calcite.expression.builtin.ExtractOperatorConversion;
import io.druid.sql.calcite.expression.builtin.FloorOperatorConversion;
import io.druid.sql.calcite.expression.builtin.LTrimOperatorConversion;
import io.druid.sql.calcite.expression.builtin.MillisToTimestampOperatorConversion;
import io.druid.sql.calcite.expression.builtin.RTrimOperatorConversion;
import io.druid.sql.calcite.expression.builtin.RegexpExtractOperatorConversion;
import io.druid.sql.calcite.expression.builtin.ReinterpretOperatorConversion;
import io.druid.sql.calcite.expression.builtin.StrposOperatorConversion;
import io.druid.sql.calcite.expression.builtin.SubstringOperatorConversion;
import io.druid.sql.calcite.expression.builtin.TimeArithmeticOperatorConversion;
import io.druid.sql.calcite.expression.builtin.TimeExtractOperatorConversion;
import io.druid.sql.calcite.expression.builtin.TimeFloorOperatorConversion;
import io.druid.sql.calcite.expression.builtin.TimeFormatOperatorConversion;
import io.druid.sql.calcite.expression.builtin.TimeParseOperatorConversion;
import io.druid.sql.calcite.expression.builtin.TimeShiftOperatorConversion;
import io.druid.sql.calcite.expression.builtin.TimestampToMillisOperatorConversion;
import io.druid.sql.calcite.expression.builtin.TrimOperatorConversion;
import io.druid.sql.calcite.expression.builtin.TruncateOperatorConversion;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.ArrayList;
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
          .add(new DirectOperatorConversion(SqlStdOperatorTable.LIKE, "like"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.LN, "log"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.LOWER, "lower"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.LOG10, "log10"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.POWER, "pow"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.REPLACE, "replace"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.SQRT, "sqrt"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.UPPER, "upper"))
          .add(new UnaryPrefixOperatorConversion(SqlStdOperatorTable.NOT, "!"))
          .add(new UnaryPrefixOperatorConversion(SqlStdOperatorTable.UNARY_MINUS, "-"))
          .add(new UnarySuffixOperatorConversion(SqlStdOperatorTable.IS_NULL, "== ''"))
          .add(new UnarySuffixOperatorConversion(SqlStdOperatorTable.IS_NOT_NULL, "!= ''"))
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
          .add(new CastOperatorConversion())
          .add(new CeilOperatorConversion())
          .add(new DateTruncOperatorConversion())
          .add(new ExtractOperatorConversion())
          .add(new FloorOperatorConversion())
          .add(new MillisToTimestampOperatorConversion())
          .add(new ReinterpretOperatorConversion())
          .add(new RegexpExtractOperatorConversion())
          .add(new StrposOperatorConversion())
          .add(new SubstringOperatorConversion())
          .add(new AliasedOperatorConversion(new SubstringOperatorConversion(), "SUBSTR"))
          .add(new TimeArithmeticOperatorConversion.TimeMinusIntervalOperatorConversion())
          .add(new TimeArithmeticOperatorConversion.TimePlusIntervalOperatorConversion())
          .add(new TimeExtractOperatorConversion())
          .add(new TimeFloorOperatorConversion())
          .add(new TimeFormatOperatorConversion())
          .add(new TimeParseOperatorConversion())
          .add(new TimeShiftOperatorConversion())
          .add(new TimestampToMillisOperatorConversion())
          .add(new TruncateOperatorConversion())
          .add(new TrimOperatorConversion())
          .add(new BTrimOperatorConversion())
          .add(new LTrimOperatorConversion())
          .add(new RTrimOperatorConversion())
          .add(new AliasedOperatorConversion(new TruncateOperatorConversion(), "TRUNC"))
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
    this.aggregators = Maps.newHashMap();
    this.operatorConversions = Maps.newHashMap();

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

  public SqlAggregator lookupAggregator(final SqlAggFunction aggFunction)
  {
    final SqlAggregator sqlAggregator = aggregators.get(OperatorKey.of(aggFunction));
    if (sqlAggregator != null && sqlAggregator.calciteFunction().equals(aggFunction)) {
      return sqlAggregator;
    } else {
      return null;
    }
  }

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

    public OperatorKey(final String name, final SqlSyntax syntax)
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
