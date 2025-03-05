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
import org.apache.druid.math.expr.vector.functional.LongBivariateDoubleLongFunction;
import org.apache.druid.math.expr.vector.functional.LongBivariateDoublesFunction;
import org.apache.druid.math.expr.vector.functional.LongBivariateLongDoubleFunction;
import org.apache.druid.math.expr.vector.functional.LongBivariateLongsFunction;

/**
 * Make a 2 argument, math processor with the following type rules
 * long, long      -> long
 * long, double    -> long
 * double, long    -> long
 * double, double  -> long
 */
public class SimpleVectorMathBivariateLongProcessorFactory extends VectorMathBivariateLongProcessorFactory
{
  private final LongBivariateLongsFunction longsFunction;
  private final LongBivariateLongDoubleFunction longDoubleFunction;
  private final LongBivariateDoubleLongFunction doubleLongFunction;
  private final LongBivariateDoublesFunction doublesFunction;

  public SimpleVectorMathBivariateLongProcessorFactory(
      LongBivariateLongsFunction longsFunction,
      LongBivariateLongDoubleFunction longDoubleFunction,
      LongBivariateDoubleLongFunction doubleLongFunction,
      LongBivariateDoublesFunction doublesFunction
  )
  {
    this.longsFunction = longsFunction;
    this.longDoubleFunction = longDoubleFunction;
    this.doubleLongFunction = doubleLongFunction;
    this.doublesFunction = doublesFunction;
  }

  @Override
  public final ExprVectorProcessor<long[]> longsProcessor(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right
  )
  {
    return new LongBivariateLongsFunctionVectorProcessor(
        left.asVectorProcessor(inspector),
        right.asVectorProcessor(inspector),
        longsFunction
    );
  }

  @Override
  public final ExprVectorProcessor<long[]> longDoubleProcessor(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right
  )
  {
    return new LongBivariateLongDoubleFunctionVectorProcessor(
        left.asVectorProcessor(inspector),
        right.asVectorProcessor(inspector),
        longDoubleFunction
    );
  }

  @Override
  public final ExprVectorProcessor<long[]> doubleLongProcessor(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right
  )
  {
    return new LongBivariateDoubleLongFunctionVectorProcessor(
        left.asVectorProcessor(inspector),
        right.asVectorProcessor(inspector),
        doubleLongFunction
    );
  }

  @Override
  public final ExprVectorProcessor<long[]> doublesProcessor(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right
  )
  {
    return new LongBivariateDoublesFunctionVectorProcessor(
        left.asVectorProcessor(inspector),
        right.asVectorProcessor(inspector),
        doublesFunction
    );
  }
}
