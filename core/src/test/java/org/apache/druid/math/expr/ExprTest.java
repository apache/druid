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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

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
                  .withIgnoredFields("outputType")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testEqualsContractForDoubleExpr()
  {
    EqualsVerifier.forClass(DoubleExpr.class)
                  .withIgnoredFields("outputType")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testEqualsContractForLongExpr()
  {
    EqualsVerifier.forClass(LongExpr.class)
                  .withIgnoredFields("outputType")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testEqualsContractForStringArrayExpr()
  {
    EqualsVerifier.forClass(StringArrayExpr.class)
                  .withIgnoredFields("outputType")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testEqualsContractForLongArrayExpr()
  {
    EqualsVerifier.forClass(LongArrayExpr.class)
                  .withIgnoredFields("outputType")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testEqualsContractForDoubleArrayExpr()
  {
    EqualsVerifier.forClass(DoubleArrayExpr.class)
                  .withIgnoredFields("outputType")
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
                  .withIgnoredFields("outputType")
                  .verify();
  }

  @Test
  public void testEqualsContractForNullDoubleExpr()
  {
    EqualsVerifier.forClass(NullDoubleExpr.class)
                  .withIgnoredFields("outputType")
                  .verify();
  }
}
