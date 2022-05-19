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
import java.util.List;

public class VectorStringProcessors
{
  public static <T> ExprVectorProcessor<T> concat(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    final ExprVectorProcessor processor;
    if (NullHandling.sqlCompatible()) {
      processor = new StringOutStringsInFunctionVectorProcessor(
          left.buildVectorized(inspector),
          right.buildVectorized(inspector),
          inspector.getMaxVectorSize()
      )
      {
        @Nullable
        @Override
        protected String processValue(@Nullable String leftVal, @Nullable String rightVal)
        {
          // in sql compatible mode, nulls are handled by super class and never make it here...
          return leftVal + rightVal;
        }
      };
    } else {
      processor = new StringOutStringsInFunctionVectorProcessor(
          left.buildVectorized(inspector),
          right.buildVectorized(inspector),
          inspector.getMaxVectorSize()
      )
      {
        @Nullable
        @Override
        protected String processValue(@Nullable String leftVal, @Nullable String rightVal)
        {
          return NullHandling.nullToEmptyIfNeeded(leftVal) + NullHandling.nullToEmptyIfNeeded(rightVal);
        }
      };
    }
    return processor;
  }

  public static <T> ExprVectorProcessor<T> concat(Expr.VectorInputBindingInspector inspector, List<Expr> inputs)
  {
    final ExprVectorProcessor<String[]>[] inputProcessors = new ExprVectorProcessor[inputs.size()];
    for (int i = 0; i < inputs.size(); i++) {
      inputProcessors[i] = CastToTypeVectorProcessor.cast(inputs.get(i).buildVectorized(inspector), ExpressionType.STRING);
    }
    final ExprVectorProcessor processor = new StringOutMultiStringInVectorProcessor(
        inputProcessors,
        inspector.getMaxVectorSize()
    )
    {
      @Override
      void processIndex(String[][] in, int i)
      {
        // Result of concatenation is null if any of the Values is null.
        // e.g. 'select CONCAT(null, "abc") as c;' will return null as per Standard SQL spec.
        String first = NullHandling.nullToEmptyIfNeeded(in[0][i]);
        if (first == null) {
          outValues[i] = null;
          return;
        }
        final StringBuilder builder = new StringBuilder(first);
        for (int inputNumber = 1; inputNumber < in.length; inputNumber++) {
          final String s = NullHandling.nullToEmptyIfNeeded(in[inputNumber][i]);
          if (s == null) {
            outValues[i] = null;
            return;
          } else {
            builder.append(s);
          }
        }
        outValues[i] = builder.toString();
      }
    };
    return processor;
  }
}
