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

package org.apache.druid.segment.join;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class JoinConditionAnalysisTest
{
  private static final String PREFIX = "j.";

  @BeforeClass
  public static void setUpStatic()
  {
    NullHandling.initializeForTests();
  }

  @Test
  public void test_forExpression_simple()
  {
    final String expression = "x == \"j.y\"";
    final JoinConditionAnalysis analysis = analyze(expression);

    Assert.assertEquals(expression, analysis.getOriginalExpression());
    Assert.assertTrue(analysis.canHashJoin());
    Assert.assertFalse(analysis.isAlwaysTrue());
    Assert.assertFalse(analysis.isAlwaysFalse());
    Assert.assertEquals(
        ImmutableList.of(Pair.of("x", "y")),
        equalitiesToPairs(analysis.getEquiConditions())
    );
    Assert.assertEquals(
        ImmutableList.of(),
        exprsToStrings(analysis.getNonEquiConditions())
    );
    Assert.assertEquals(analysis.getRightEquiConditionKeys(), ImmutableSet.of("y"));
  }

  @Test
  public void test_forExpression_simpleFlipped()
  {
    final String expression = "\"j.y\" == x";
    final JoinConditionAnalysis analysis = analyze(expression);

    Assert.assertEquals(expression, analysis.getOriginalExpression());
    Assert.assertTrue(analysis.canHashJoin());
    Assert.assertFalse(analysis.isAlwaysTrue());
    Assert.assertFalse(analysis.isAlwaysFalse());
    Assert.assertEquals(
        ImmutableList.of(Pair.of("x", "y")),
        equalitiesToPairs(analysis.getEquiConditions())
    );
    Assert.assertEquals(
        ImmutableList.of(),
        exprsToStrings(analysis.getNonEquiConditions())
    );
    Assert.assertEquals(analysis.getRightEquiConditionKeys(), ImmutableSet.of("y"));
  }

  @Test
  public void test_forExpression_leftFunction()
  {
    final String expression = "x + y == \"j.z\"";
    final JoinConditionAnalysis analysis = analyze(expression);

    Assert.assertEquals(expression, analysis.getOriginalExpression());
    Assert.assertTrue(analysis.canHashJoin());
    Assert.assertFalse(analysis.isAlwaysTrue());
    Assert.assertFalse(analysis.isAlwaysFalse());
    Assert.assertEquals(
        ImmutableList.of(Pair.of("(+ x y)", "z")),
        equalitiesToPairs(analysis.getEquiConditions())
    );
    Assert.assertEquals(
        ImmutableList.of(),
        exprsToStrings(analysis.getNonEquiConditions())
    );
    Assert.assertEquals(analysis.getRightEquiConditionKeys(), ImmutableSet.of("z"));
  }

  @Test
  public void test_forExpression_rightFunction()
  {
    final String expression = "\"j.x\" + \"j.y\" == z";
    final JoinConditionAnalysis analysis = analyze(expression);

    Assert.assertEquals(expression, analysis.getOriginalExpression());
    Assert.assertFalse(analysis.canHashJoin());
    Assert.assertFalse(analysis.isAlwaysTrue());
    Assert.assertFalse(analysis.isAlwaysFalse());
    Assert.assertEquals(
        ImmutableList.of(),
        equalitiesToPairs(analysis.getEquiConditions())
    );
    Assert.assertEquals(
        ImmutableList.of("(== (+ j.x j.y) z)"),
        exprsToStrings(analysis.getNonEquiConditions())
    );
    Assert.assertTrue(analysis.getRightEquiConditionKeys().isEmpty());
  }

  @Test
  public void test_forExpression_mixedFunction()
  {
    final String expression = "x + \"j.y\" == \"j.z\"";
    final JoinConditionAnalysis analysis = analyze(expression);

    Assert.assertEquals(expression, analysis.getOriginalExpression());
    Assert.assertFalse(analysis.canHashJoin());
    Assert.assertFalse(analysis.isAlwaysTrue());
    Assert.assertFalse(analysis.isAlwaysFalse());
    Assert.assertEquals(
        ImmutableList.of(),
        equalitiesToPairs(analysis.getEquiConditions())
    );
    Assert.assertEquals(
        ImmutableList.of("(== (+ x j.y) j.z)"),
        exprsToStrings(analysis.getNonEquiConditions())
    );
    Assert.assertTrue(analysis.getRightEquiConditionKeys().isEmpty());
  }

  @Test
  public void test_forExpression_trueConstant()
  {
    final String expression = "1 + 1";
    final JoinConditionAnalysis analysis = analyze(expression);

    Assert.assertEquals(expression, analysis.getOriginalExpression());
    Assert.assertTrue(analysis.canHashJoin());
    Assert.assertTrue(analysis.isAlwaysTrue());
    Assert.assertFalse(analysis.isAlwaysFalse());
    Assert.assertEquals(
        ImmutableList.of(),
        equalitiesToPairs(analysis.getEquiConditions())
    );
    Assert.assertEquals(
        ImmutableList.of("2"),
        exprsToStrings(analysis.getNonEquiConditions())
    );
    Assert.assertTrue(analysis.getRightEquiConditionKeys().isEmpty());
  }

  @Test
  public void test_forExpression_falseConstant()
  {
    final String expression = "0";
    final JoinConditionAnalysis analysis = analyze(expression);

    Assert.assertEquals(expression, analysis.getOriginalExpression());
    Assert.assertTrue(analysis.canHashJoin());
    Assert.assertFalse(analysis.isAlwaysTrue());
    Assert.assertTrue(analysis.isAlwaysFalse());
    Assert.assertEquals(
        ImmutableList.of(),
        equalitiesToPairs(analysis.getEquiConditions())
    );
    Assert.assertEquals(
        ImmutableList.of("0"),
        exprsToStrings(analysis.getNonEquiConditions())
    );
    Assert.assertTrue(analysis.getRightEquiConditionKeys().isEmpty());
  }

  @Test
  public void test_forExpression_onlyLeft()
  {
    final String expression = "x == 1";
    final JoinConditionAnalysis analysis = analyze(expression);

    Assert.assertEquals(expression, analysis.getOriginalExpression());
    Assert.assertFalse(analysis.canHashJoin());
    Assert.assertFalse(analysis.isAlwaysTrue());
    Assert.assertFalse(analysis.isAlwaysFalse());
    Assert.assertEquals(
        ImmutableList.of(),
        equalitiesToPairs(analysis.getEquiConditions())
    );
    Assert.assertEquals(
        ImmutableList.of("(== x 1)"),
        exprsToStrings(analysis.getNonEquiConditions())
    );
    Assert.assertTrue(analysis.getRightEquiConditionKeys().isEmpty());
  }

  @Test
  public void test_forExpression_onlyRight()
  {
    final String expression = "\"j.x\" == 1";
    final JoinConditionAnalysis analysis = analyze(expression);

    Assert.assertEquals(expression, analysis.getOriginalExpression());
    Assert.assertTrue(analysis.canHashJoin());
    Assert.assertFalse(analysis.isAlwaysTrue());
    Assert.assertFalse(analysis.isAlwaysFalse());
    Assert.assertEquals(
        ImmutableList.of(Pair.of("1", "x")),
        equalitiesToPairs(analysis.getEquiConditions())
    );
    Assert.assertEquals(
        ImmutableList.of(),
        exprsToStrings(analysis.getNonEquiConditions())
    );
    Assert.assertEquals(analysis.getRightEquiConditionKeys(), ImmutableSet.of("x"));
  }

  @Test
  public void test_forExpression_andOfThreeConditions()
  {
    final String expression = "(x == \"j.y\") && (x + y == \"j.z\") && (z == \"j.zz\")";
    final JoinConditionAnalysis analysis = analyze(expression);

    Assert.assertEquals(expression, analysis.getOriginalExpression());
    Assert.assertTrue(analysis.canHashJoin());
    Assert.assertFalse(analysis.isAlwaysTrue());
    Assert.assertFalse(analysis.isAlwaysFalse());
    Assert.assertEquals(
        ImmutableList.of(Pair.of("x", "y"), Pair.of("(+ x y)", "z"), Pair.of("z", "zz")),
        equalitiesToPairs(analysis.getEquiConditions())
    );
    Assert.assertEquals(
        ImmutableList.of(),
        exprsToStrings(analysis.getNonEquiConditions())
    );
    Assert.assertEquals(analysis.getRightEquiConditionKeys(), ImmutableSet.of("y", "z", "zz"));
  }

  @Test
  public void test_forExpression_mixedAndWithOr()
  {
    final String expression = "(x == \"j.y\") && ((x + y == \"j.z\") || (z == \"j.zz\"))";
    final JoinConditionAnalysis analysis = analyze(expression);

    Assert.assertEquals(expression, analysis.getOriginalExpression());
    Assert.assertFalse(analysis.canHashJoin());
    Assert.assertFalse(analysis.isAlwaysTrue());
    Assert.assertFalse(analysis.isAlwaysFalse());
    Assert.assertEquals(
        ImmutableList.of(Pair.of("x", "y")),
        equalitiesToPairs(analysis.getEquiConditions())
    );
    Assert.assertEquals(
        ImmutableList.of("(|| (== (+ x y) j.z) (== z j.zz))"),
        exprsToStrings(analysis.getNonEquiConditions())
    );
    Assert.assertEquals(analysis.getRightEquiConditionKeys(), ImmutableSet.of("y"));
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(JoinConditionAnalysis.class)
                  .usingGetClass()
                  .withIgnoredFields(
                          // These fields are tightly coupled with originalExpression
                          "equiConditions", "nonEquiConditions",
                          // These fields are calculated from other other fields in the class
                          "isAlwaysTrue", "isAlwaysFalse", "canHashJoin", "rightKeyColumns")
                  .verify();
  }

  private static JoinConditionAnalysis analyze(final String expression)
  {
    return JoinConditionAnalysis.forExpression(expression, PREFIX, ExprMacroTable.nil());
  }

  private static Pair<String, String> equalityToPair(final Equality equality)
  {
    return Pair.of(equality.getLeftExpr().toString(), equality.getRightColumn());
  }

  private static List<Pair<String, String>> equalitiesToPairs(final List<Equality> equalities)
  {
    return equalities.stream().map(JoinConditionAnalysisTest::equalityToPair).collect(Collectors.toList());
  }

  private static List<String> exprsToStrings(final List<Expr> exprs)
  {
    return exprs.stream().map(String::valueOf).collect(Collectors.toList());
  }
}
