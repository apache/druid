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

package org.apache.druid.sql.calcite.rule;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.sql.calcite.expression.builtin.ConcatOperatorConversion;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;

public class FilterDecomposeConcatRuleTest extends InitializedNullHandlingTest
{
  private final RelDataTypeFactory typeFactory = DruidTypeSystem.TYPE_FACTORY;
  private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
  private final RexShuttle shuttle = new FilterDecomposeConcatRule.DecomposeConcatShuttle(rexBuilder);

  @Test
  public void test_notConcat()
  {
    final RexNode call =
        equals(
            rexBuilder.makeCall(SqlStdOperatorTable.LOWER, inputRef(0)),
            literal("2")
        );

    Assert.assertEquals(call, shuttle.apply(call));
  }

  @Test
  public void test_oneInput()
  {
    final RexNode concatCall =
        concat(literal("it's "), inputRef(0));

    Assert.assertEquals(
        and(equals(inputRef(0), literal("2"))),
        shuttle.apply(equals(concatCall, literal("it's 2")))
    );
  }

  @Test
  public void test_oneInput_lhsLiteral()
  {
    final RexNode concatCall =
        concat(literal("it's "), inputRef(0));

    Assert.assertEquals(
        and(equals(inputRef(0), literal("2"))),
        shuttle.apply(equals(literal("it's 2"), concatCall))
    );
  }

  @Test
  public void test_oneInput_noLiteral()
  {
    final RexNode concatCall = concat(inputRef(0));

    Assert.assertEquals(
        and(equals(inputRef(0), literal("it's 2"))),
        shuttle.apply(equals(literal("it's 2"), concatCall))
    );
  }

  @Test
  public void test_twoInputs()
  {
    final RexNode concatCall =
        concat(inputRef(0), literal("x"), inputRef(1));

    Assert.assertEquals(
        and(equals(inputRef(0), literal("2")), equals(inputRef(1), literal("3"))),
        shuttle.apply(equals(concatCall, literal("2x3")))
    );
  }

  @Test
  public void test_twoInputs_castNumberInputRef()
  {
    // CAST(x AS VARCHAR) when x is BIGINT
    final RexNode numericInputRef = rexBuilder.makeCast(
        typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true),
        rexBuilder.makeInputRef(
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true),
            0
        )
    );

    final RexNode concatCall =
        concat(numericInputRef, literal("x"), inputRef(1));

    Assert.assertEquals(
        and(
            equals(
                numericInputRef,
                literal("2")
            ),
            equals(
                inputRef(1),
                literal("3")
            )
        ),
        shuttle.apply(equals(concatCall, literal("2x3")))
    );
  }

  @Test
  public void test_twoInputs_notEquals()
  {
    final RexNode call =
        notEquals(
            concat(inputRef(0), literal("x"), inputRef(1)),
            literal("2x3")
        );

    Assert.assertEquals(
        rexBuilder.makeCall(
            SqlStdOperatorTable.NOT,
            and(equals(inputRef(0), literal("2")), equals(inputRef(1), literal("3")))
        ),
        shuttle.apply(call)
    );
  }

  @Test
  public void test_twoInputs_castNumberLiteral()
  {
    final RexNode three = rexBuilder.makeCast(
        typeFactory.createSqlType(SqlTypeName.VARCHAR),
        rexBuilder.makeExactLiteral(BigDecimal.valueOf(3L))
    );

    final RexNode concatCall =
        concat(inputRef(0), three, inputRef(1), literal("4"));

    Assert.assertEquals(
        and(equals(inputRef(0), literal("x")), equals(inputRef(1), literal("y"))),
        shuttle.apply(equals(concatCall, literal("x3y4")))
    );
  }

  @Test
  public void test_twoInputs_noLiteral()
  {
    final RexNode call = equals(concat(inputRef(0), inputRef(1)), literal("2x3"));
    Assert.assertEquals(call, shuttle.apply(call));
  }

  @Test
  public void test_twoInputs_isNull()
  {
    final RexNode call =
        isNull(concat(inputRef(0), literal("x"), inputRef(1)));

    Assert.assertEquals(
        NullHandling.sqlCompatible()
        ? or(isNull(inputRef(0)), isNull(inputRef(1)))
        : rexBuilder.makeLiteral(false),
        shuttle.apply(call)
    );
  }

  @Test
  public void test_twoInputs_isNotNull()
  {
    final RexNode call =
        notNull(concat(inputRef(0), literal("x"), inputRef(1)));

    Assert.assertEquals(
        rexBuilder.makeCall(
            SqlStdOperatorTable.NOT,
            NullHandling.sqlCompatible()
            ? or(isNull(inputRef(0)), isNull(inputRef(1)))
            : rexBuilder.makeLiteral(false)
        ),
        shuttle.apply(call)
    );
  }

  @Test
  public void test_twoInputs_tooManyXes()
  {
    final RexNode call =
        equals(
            concat(inputRef(0), literal("x"), inputRef(1)),
            literal("2xx3") // ambiguous match
        );

    Assert.assertEquals(call, shuttle.apply(call));
  }

  @Test
  public void test_twoInputs_notEnoughXes()
  {
    final RexNode call =
        equals(
            concat(inputRef(0), literal("x"), inputRef(1)),
            literal("2z3") // doesn't match concat pattern
        );

    final RexLiteral unknown = rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.BOOLEAN));
    Assert.assertEquals(
        NullHandling.sqlCompatible()
        ? or(
            and(isNull(inputRef(0)), unknown),
            and(isNull(inputRef(1)), unknown)
        )
        : rexBuilder.makeLiteral(false),
        shuttle.apply(call)
    );
  }

  @Test
  public void test_twoInputs_delimitersWrongOrder()
  {
    final RexNode call =
        equals(
            concat(literal("z"), inputRef(0), literal("x"), inputRef(1)),
            literal("x2z3") // doesn't match concat pattern
        );

    final RexLiteral unknown = rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.BOOLEAN));
    Assert.assertEquals(
        NullHandling.sqlCompatible()
        ? or(
            and(isNull(inputRef(0)), unknown),
            and(isNull(inputRef(1)), unknown)
        )
        : rexBuilder.makeLiteral(false),
        shuttle.apply(call)
    );
  }

  @Test
  public void test_twoInputs_emptyDelimiter()
  {
    final RexNode call =
        equals(
            concat(inputRef(0), literal(""), inputRef(1)),
            literal("23") // must be recognized as ambiguous
        );

    Assert.assertEquals(call, shuttle.apply(call));
  }

  @Test
  public void test_twoInputs_ambiguousOverlappingDeliminters()
  {
    final RexNode call =
        equals(
            concat(inputRef(0), literal("--"), inputRef(1)),
            literal("2---3") // must be recognized as ambiguous
        );

    Assert.assertEquals(call, shuttle.apply(call));
  }

  @Test
  public void test_twoInputs_impossibleOverlappingDelimiters()
  {
    final RexNode call =
        equals(
            concat(inputRef(0), literal("--"), inputRef(1), literal("--")),
            literal("2---3") // must be recognized as impossible
        );

    final RexLiteral unknown = rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.BOOLEAN));
    Assert.assertEquals(
        NullHandling.sqlCompatible()
        ? or(
            and(isNull(inputRef(0)), unknown),
            and(isNull(inputRef(1)), unknown)
        )
        : rexBuilder.makeLiteral(false),
        shuttle.apply(call)
    );
  }

  @Test
  public void test_threeInputs_delimitersIgnoredWhenOutOfPosition()
  {
    final RexNode call =
        equals(
            concat(inputRef(0), literal(" ("), inputRef(1), literal("x"), inputRef(2), literal(")")),
            literal("xxx (4x5)") // unambiguous, because 'x' before ' (' can be ignored
        );

    Assert.assertEquals(
        and(
            equals(inputRef(0), literal("xxx")),
            equals(inputRef(1), literal("4")),
            equals(inputRef(2), literal("5"))
        ),
        shuttle.apply(call)
    );
  }

  @Test
  public void test_twoInputs_backToBackLiterals()
  {
    final RexNode concatCall =
        concat(inputRef(0), literal("x"), literal("y"), inputRef(1));

    Assert.assertEquals(
        and(equals(inputRef(0), literal("2")), equals(inputRef(1), literal("3"))),
        shuttle.apply(equals(concatCall, literal("2xy3")))
    );
  }

  private RexNode concat(RexNode... args)
  {
    return rexBuilder.makeCall(ConcatOperatorConversion.SQL_FUNCTION, args);
  }

  private RexNode inputRef(int i)
  {
    return rexBuilder.makeInputRef(
        typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            true
        ),
        i
    );
  }

  private RexNode or(RexNode... args)
  {
    return RexUtil.composeDisjunction(rexBuilder, Arrays.asList(args));
  }

  private RexNode and(RexNode... args)
  {
    return RexUtil.composeConjunction(rexBuilder, Arrays.asList(args));
  }

  private RexNode equals(RexNode arg, RexNode value)
  {
    return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, arg, value);
  }

  private RexNode notEquals(RexNode arg, RexNode value)
  {
    return rexBuilder.makeCall(SqlStdOperatorTable.NOT_EQUALS, arg, value);
  }

  private RexNode isNull(RexNode arg)
  {
    return rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, arg);
  }

  private RexNode notNull(RexNode arg)
  {
    return rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, arg);
  }

  private RexNode literal(String s)
  {
    return rexBuilder.makeLiteral(s);
  }
}
