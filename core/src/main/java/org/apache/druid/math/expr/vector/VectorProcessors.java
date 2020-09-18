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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprType;

import javax.annotation.Nullable;
import java.util.Arrays;

public class VectorProcessors
{
  public static <T> VectorExprProcessor<T> constantString(@Nullable String constant, int maxVectorSize)
  {
    final String[] strings = new String[maxVectorSize];
    Arrays.fill(strings, constant);
    final StringVectorExprEval eval = new StringVectorExprEval(strings);
    return new VectorExprProcessor<T>()
    {
      @Override
      public VectorExprEval<T> evalVector(Expr.VectorInputBinding bindings)
      {
        return (VectorExprEval<T>) eval;
      }

      @Override
      public ExprType getOutputType()
      {
        return ExprType.STRING;
      }
    };
  }

  public static <T> VectorExprProcessor<T> constantDouble(@Nullable Double constant, int maxVectorSize)
  {
    final double[] doubles = new double[maxVectorSize];
    final boolean[] nulls;
    if (constant == null) {
      nulls = new boolean[maxVectorSize];
      Arrays.fill(nulls, true);
    } else {
      nulls = null;
      Arrays.fill(doubles, constant);
    }
    final DoubleVectorExprEval eval = new DoubleVectorExprEval(doubles, nulls);
    return new VectorExprProcessor<T>()
    {
      @Override
      public VectorExprEval<T> evalVector(Expr.VectorInputBinding bindings)
      {
        return (VectorExprEval<T>) eval;
      }

      @Override
      public ExprType getOutputType()
      {
        return ExprType.DOUBLE;
      }
    };
  }

  public static <T> VectorExprProcessor<T> constantLong(@Nullable Long constant, int maxVectorSize)
  {
    final long[] longs = new long[maxVectorSize];
    final boolean[] nulls;
    if (constant == null) {
      nulls = new boolean[maxVectorSize];
      Arrays.fill(nulls, true);
    } else {
      nulls = null;
      Arrays.fill(longs, constant);
    }
    final LongVectorExprEval eval = new LongVectorExprEval(longs, nulls);
    return new VectorExprProcessor<T>()
    {
      @Override
      public VectorExprEval<T> evalVector(Expr.VectorInputBinding bindings)
      {
        return (VectorExprEval<T>) eval;
      }

      @Override
      public ExprType getOutputType()
      {
        return ExprType.LONG;
      }
    };
  }

  public static <T> VectorExprProcessor<T> parseLong(Expr.VectorInputBindingTypes inputTypes, Expr arg, int radix)
  {
    final VectorExprProcessor<?> processor = new LongOutStringInFunctionVectorProcessor(
        CastToTypeVectorProcessor.castToType(arg.buildVectorized(inputTypes), ExprType.STRING),
        inputTypes.getMaxVectorSize()
    )
    {
      @Override
      public void processIndex(String[] strings, long[] longs, boolean[] outputNulls, int i)
      {
        try {
          longs[i] = Long.parseLong(strings[i], radix);
          outputNulls[i] = false;
        }
        catch (NumberFormatException e) {
          longs[i] = 0L;
          outputNulls[i] = NullHandling.sqlCompatible();
        }
      }
    };

    return (VectorExprProcessor<T>) processor;
  }
}
