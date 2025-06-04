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

package org.apache.druid.math.expr.vector;

import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.vector.functional.DoubleBivariateDoubleLongFunction;
import org.apache.druid.math.expr.vector.functional.DoubleBivariateDoublesFunction;
import org.apache.druid.math.expr.vector.functional.DoubleBivariateLongDoubleFunction;
import org.apache.druid.math.expr.vector.functional.DoubleBivariateLongsFunction;

/**
 * Make a 2 argument, math processor with the following type rules
 * long, long      -> double
 * long, double    -> double
 * double, long    -> double
 * double, double  -> double
 */
public class SimpleVectorMathBivariateDoubleProcessorFactory extends VectorMathBivariateDoubleProcessorFactory
{
  private final DoubleBivariateLongsFunction longsFunction;
  private final DoubleBivariateLongDoubleFunction longDoubleFunction;
  private final DoubleBivariateDoubleLongFunction doubleLongFunction;
  private final DoubleBivariateDoublesFunction doublesFunction;

  public SimpleVectorMathBivariateDoubleProcessorFactory(
      DoubleBivariateLongsFunction longsFunction,
      DoubleBivariateLongDoubleFunction longDoubleFunction,
      DoubleBivariateDoubleLongFunction doubleLongFunction,
      DoubleBivariateDoublesFunction doublesFunction
  )
  {
    this.longsFunction = longsFunction;
    this.longDoubleFunction = longDoubleFunction;
    this.doubleLongFunction = doubleLongFunction;
    this.doublesFunction = doublesFunction;
  }

  @Override
  public final ExprVectorProcessor<double[]> longsProcessor(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right
  )
  {
    return new DoubleBivariateLongsFunctionVectorProcessor(
        left.asVectorProcessor(inspector),
        right.asVectorProcessor(inspector),
        longsFunction
    );
  }

  @Override
  public final ExprVectorProcessor<double[]> longDoubleProcessor(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right
  )
  {
    return new DoubleBivariateLongDoubleFunctionVectorProcessor(
        left.asVectorProcessor(inspector),
        right.asVectorProcessor(inspector),
        longDoubleFunction
    );
  }

  @Override
  public final ExprVectorProcessor<double[]> doubleLongProcessor(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right
  )
  {
    return new DoubleBivariateDoubleLongFunctionVectorProcessor(
        left.asVectorProcessor(inspector),
        right.asVectorProcessor(inspector),
        doubleLongFunction
    );
  }

  @Override
  public final ExprVectorProcessor<double[]> doublesProcessor(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right
  )
  {
    return new DoubleBivariateDoublesFunctionVectorProcessor(
        left.asVectorProcessor(inspector),
        right.asVectorProcessor(inspector),
        doublesFunction
    );
  }
}
