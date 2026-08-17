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
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.math.expr.vector.functional.DoubleBivariateDoubleLongFunction;
import org.apache.druid.math.expr.vector.functional.DoubleBivariateDoublesFunction;
import org.apache.druid.math.expr.vector.functional.DoubleBivariateLongDoubleFunction;
import org.apache.druid.math.expr.vector.functional.LongBivariateLongsFunction;
import org.apache.druid.math.expr.vector.simd.SimdProcessors;
import org.apache.druid.math.expr.vector.simd.SimdSupportedBinaryOp;

import javax.annotation.Nullable;

/**
 * Make a 2 argument, math processor with the following type rules
 * long, long      -> long
 * long, double    -> double
 * double, long    -> double
 * double, double  -> double
 *
 * If a non-null {@link SimdSupportedBinaryOp} is supplied to the constructor and
 * {@link ExpressionProcessing#useVectorApi()} is true, this factory will return SIMD-specialized processors backed
 * by the JDK incubator {@code jdk.incubator.vector} API instead of the standard scalar implementations.
 */
public class SimpleVectorMathBivariateProcessorFactory extends VectorMathBivariateProcessorFactory
{
  private final LongBivariateLongsFunction longsFunction;
  private final DoubleBivariateLongDoubleFunction longDoubleFunction;
  private final DoubleBivariateDoubleLongFunction doubleLongFunction;
  private final DoubleBivariateDoublesFunction doublesFunction;
  @Nullable
  private final SimdSupportedBinaryOp simdOp;

  protected SimpleVectorMathBivariateProcessorFactory(
      LongBivariateLongsFunction longsFunction,
      DoubleBivariateLongDoubleFunction longDoubleFunction,
      DoubleBivariateDoubleLongFunction doubleLongFunction,
      DoubleBivariateDoublesFunction doublesFunction
  )
  {
    this(longsFunction, longDoubleFunction, doubleLongFunction, doublesFunction, null);
  }

  protected SimpleVectorMathBivariateProcessorFactory(
      LongBivariateLongsFunction longsFunction,
      DoubleBivariateLongDoubleFunction longDoubleFunction,
      DoubleBivariateDoubleLongFunction doubleLongFunction,
      DoubleBivariateDoublesFunction doublesFunction,
      @Nullable SimdSupportedBinaryOp simdOp
  )
  {
    this.longsFunction = longsFunction;
    this.longDoubleFunction = longDoubleFunction;
    this.doubleLongFunction = doubleLongFunction;
    this.doublesFunction = doublesFunction;
    this.simdOp = simdOp;
  }

  @Override
  public final ExprVectorProcessor<long[]> longsProcessor(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    if (simdOp != null && simdOp.supportsLongLong() && ExpressionProcessing.useVectorApi()) {
      return SimdProcessors.makeLongLong(
          left.asVectorProcessor(inspector),
          right.asVectorProcessor(inspector),
          simdOp,
          longsFunction
      );
    }
    return new LongBivariateLongsFunctionVectorProcessor(
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
    if (simdOp != null && ExpressionProcessing.useVectorApi()) {
      return SimdProcessors.makeLongDouble(
          left.asVectorProcessor(inspector),
          right.asVectorProcessor(inspector),
          simdOp,
          longDoubleFunction
      );
    }
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
    if (simdOp != null && ExpressionProcessing.useVectorApi()) {
      return SimdProcessors.makeDoubleLong(
          left.asVectorProcessor(inspector),
          right.asVectorProcessor(inspector),
          simdOp,
          doubleLongFunction
      );
    }
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
    if (simdOp != null && ExpressionProcessing.useVectorApi()) {
      return SimdProcessors.makeDoubleDouble(
          left.asVectorProcessor(inspector),
          right.asVectorProcessor(inspector),
          simdOp,
          doublesFunction
      );
    }
    return new DoubleBivariateDoublesFunctionVectorProcessor(
        left.asVectorProcessor(inspector),
        right.asVectorProcessor(inspector),
        doublesFunction
    );
  }
}
