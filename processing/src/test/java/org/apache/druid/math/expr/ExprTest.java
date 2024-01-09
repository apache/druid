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

package org.apache.druid.math.expr;

import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ExprTest
{
  @Test
  public void testEqualsContractForBinOrExpr()
  {
    EqualsVerifier.forClass(BinOrExpr.class).usingGetClass().verify();
  }

  @Test
  public void testEqualsContractForBinGtExpr()
  {
    EqualsVerifier.forClass(BinGtExpr.class).usingGetClass().verify();
  }

  @Test
  public void testEqualsContractForBinMinusExpr()
  {
    EqualsVerifier.forClass(BinMinusExpr.class).usingGetClass().verify();
  }

  @Test
  public void testEqualsContractForBinPowExpr()
  {
    EqualsVerifier.forClass(BinPowExpr.class).usingGetClass().verify();
  }

  @Test
  public void testEqualsContractForBinMulExpr()
  {
    EqualsVerifier.forClass(BinMulExpr.class).usingGetClass().verify();
  }

  @Test
  public void testEqualsContractForBinDivExpr()
  {
    EqualsVerifier.forClass(BinDivExpr.class).usingGetClass().verify();
  }

  @Test
  public void testEqualsContractForBinModuloExpr()
  {
    EqualsVerifier.forClass(BinModuloExpr.class).usingGetClass().verify();
  }

  @Test
  public void testEqualsContractForBinPlusExpr()
  {
    EqualsVerifier.forClass(BinPlusExpr.class).usingGetClass().verify();
  }

  @Test
  public void testEqualsContractForBinLtExpr()
  {
    EqualsVerifier.forClass(BinLtExpr.class).usingGetClass().verify();
  }

  @Test
  public void testEqualsContractForBinGeqExpr()
  {
    EqualsVerifier.forClass(BinGeqExpr.class).usingGetClass().verify();
  }

  @Test
  public void testEqualsContractForBinEqExpr()
  {
    EqualsVerifier.forClass(BinEqExpr.class).usingGetClass().verify();
  }

  @Test
  public void testEqualsContractForBinNeqExpr()
  {
    EqualsVerifier.forClass(BinNeqExpr.class).usingGetClass().verify();
  }

  @Test
  public void testEqualsContractForBinAndExpr()
  {
    EqualsVerifier.forClass(BinAndExpr.class).usingGetClass().verify();
  }

  @Test
  public void testEqualsContractForFunctionExpr()
  {
    EqualsVerifier.forClass(FunctionExpr.class).usingGetClass().withIgnoredFields("function").verify();
  }

  @Test
  public void testEqualsContractForApplyFunctionExpr()
  {
    EqualsVerifier.forClass(ApplyFunctionExpr.class)
                  .usingGetClass()
                  .withIgnoredFields("function", "bindingAnalysis", "lambdaBindingAnalysis", "argsBindingAnalyses")
                  .verify();
  }

  @Test
  public void testEqualsContractForUnaryNotExpr()
  {
    EqualsVerifier.forClass(UnaryNotExpr.class).withIgnoredFields("op").usingGetClass().verify();
  }

  @Test
  public void testEqualsContractForUnaryMinusExpr()
  {
    EqualsVerifier.forClass(UnaryMinusExpr.class).withIgnoredFields("op").usingGetClass().verify();
  }

  @Test
  public void testEqualsContractForStringExpr()
  {
    EqualsVerifier.forClass(StringExpr.class)
                  .withIgnoredFields("outputType", "expr")
                  .withPrefabValues(ExpressionType.class, ExpressionType.STRING, ExpressionType.DOUBLE)
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testEqualsContractForDoubleExpr()
  {
    EqualsVerifier.forClass(DoubleExpr.class)
                  .withIgnoredFields("outputType", "expr")
                  .withPrefabValues(ExpressionType.class, ExpressionType.DOUBLE, ExpressionType.LONG)
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testEqualsContractForLongExpr()
  {
    EqualsVerifier.forClass(LongExpr.class)
                  .withIgnoredFields("outputType", "expr")
                  .withPrefabValues(ExpressionType.class, ExpressionType.LONG, ExpressionType.STRING)
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testEqualsContractForArrayExpr()
  {
    EqualsVerifier.forClass(ArrayExpr.class)
                  .withPrefabValues(Object.class, new Object[]{1L}, new Object[0])
                  .withPrefabValues(ExpressionType.class, ExpressionType.LONG_ARRAY, ExpressionType.DOUBLE_ARRAY)
                  .withNonnullFields("outputType")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testEqualsContractForComplexExpr()
  {
    EqualsVerifier.forClass(ComplexExpr.class)
                  .withPrefabValues(Object.class, new Object[]{1L}, new Object[0])
                  .withPrefabValues(
                      ExpressionType.class,
                      ExpressionTypeFactory.getInstance().ofComplex("foo"),
                      ExpressionTypeFactory.getInstance().ofComplex("bar")
                  )
                  .withNonnullFields("outputType")
                  .withIgnoredFields("expr")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testEqualsContractForIdentifierExpr()
  {
    EqualsVerifier.forClass(IdentifierExpr.class).usingGetClass().withIgnoredFields("binding").verify();
  }

  @Test
  public void testEqualsContractForLambdaExpr()
  {
    EqualsVerifier.forClass(LambdaExpr.class).usingGetClass().verify();
  }

  @Test
  public void testEqualsContractForNullLongExpr()
  {
    EqualsVerifier.forClass(NullLongExpr.class)
                  .withIgnoredFields("outputType", "value")
                  .withPrefabValues(ExpressionType.class, ExpressionType.LONG, ExpressionType.STRING)
                  .verify();
  }

  @Test
  public void testEqualsContractForNullDoubleExpr()
  {
    EqualsVerifier.forClass(NullDoubleExpr.class)
                  .withIgnoredFields("outputType", "value")
                  .withPrefabValues(ExpressionType.class, ExpressionType.DOUBLE, ExpressionType.STRING)
                  .verify();
  }

  @Test
  public void testShuttleVisitAll()
  {
    final List<Expr> visitedExprs = new ArrayList<>();

    final Expr.Shuttle shuttle = expr -> {
      visitedExprs.add(expr);
      return expr;
    };

    shuttle.visitAll(Collections.emptyList());
    Assert.assertEquals("Visiting an empty list", Collections.emptyList(), visitedExprs);

    final List<Expr> oneIdentifier = Collections.singletonList(new IdentifierExpr("ident"));
    visitedExprs.clear();
    shuttle.visitAll(oneIdentifier);
    Assert.assertEquals("One identifier", oneIdentifier, visitedExprs);

    final List<Expr> twoIdentifiers = ImmutableList.of(
        new IdentifierExpr("ident1"),
        new IdentifierExpr("ident2")
    );
    visitedExprs.clear();
    shuttle.visitAll(twoIdentifiers);
    Assert.assertEquals("Two identifiers", twoIdentifiers, visitedExprs);
  }
}
