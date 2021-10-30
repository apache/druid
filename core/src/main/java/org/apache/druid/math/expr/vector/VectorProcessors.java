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
import org.apache.druid.math.expr.ExpressionType;

import javax.annotation.Nullable;
import java.util.Arrays;

public class VectorProcessors
{
  public static <T> ExprVectorProcessor<T> constantString(@Nullable String constant, int maxVectorSize)
  {
    final String[] strings = new String[maxVectorSize];
    Arrays.fill(strings, constant);
    final ExprEvalStringVector eval = new ExprEvalStringVector(strings);
    return new ExprVectorProcessor<T>()
    {
      @Override
      public ExprEvalVector<T> evalVector(Expr.VectorInputBinding bindings)
      {
        return (ExprEvalVector<T>) eval;
      }

      @Override
      public ExpressionType getOutputType()
      {
        return ExpressionType.STRING;
      }
    };
  }

  public static <T> ExprVectorProcessor<T> constantDouble(@Nullable Double constant, int maxVectorSize)
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
    final ExprEvalDoubleVector eval = new ExprEvalDoubleVector(doubles, nulls);
    return new ExprVectorProcessor<T>()
    {
      @Override
      public ExprEvalVector<T> evalVector(Expr.VectorInputBinding bindings)
      {
        return (ExprEvalVector<T>) eval;
      }

      @Override
      public ExpressionType getOutputType()
      {
        return ExpressionType.DOUBLE;
      }
    };
  }

  public static <T> ExprVectorProcessor<T> constantLong(@Nullable Long constant, int maxVectorSize)
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
    final ExprEvalLongVector eval = new ExprEvalLongVector(longs, nulls);
    return new ExprVectorProcessor<T>()
    {
      @Override
      public ExprEvalVector<T> evalVector(Expr.VectorInputBinding bindings)
      {
        return (ExprEvalVector<T>) eval;
      }

      @Override
      public ExpressionType getOutputType()
      {
        return ExpressionType.LONG;
      }
    };
  }

  public static <T> ExprVectorProcessor<T> parseLong(Expr.VectorInputBindingInspector inspector, Expr arg, int radix)
  {
    final ExprVectorProcessor<?> processor = new LongOutStringInFunctionVectorProcessor(
        CastToTypeVectorProcessor.cast(arg.buildVectorized(inspector), ExpressionType.STRING),
        inspector.getMaxVectorSize()
    )
    {
      @Override
      public void processIndex(String[] strings, long[] longs, boolean[] outputNulls, int i)
      {
        try {
          final String input = strings[i];
          if (radix == 16 && (input.startsWith("0x") || input.startsWith("0X"))) {
            // Strip leading 0x from hex strings.
            longs[i] = Long.parseLong(input.substring(2), radix);
          } else {
            longs[i] = Long.parseLong(input, radix);
          }
          outputNulls[i] = false;
        }
        catch (NumberFormatException e) {
          longs[i] = 0L;
          outputNulls[i] = NullHandling.sqlCompatible();
        }
      }
    };

    return (ExprVectorProcessor<T>) processor;
  }

  private VectorProcessors()
  {
    // No instantiation
  }
}
