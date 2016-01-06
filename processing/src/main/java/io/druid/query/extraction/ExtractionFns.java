/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.extraction;

import com.google.common.base.Function;
import com.google.common.base.Functions;

import javax.annotation.Nullable;
import java.util.Arrays;

/**
 */
public class ExtractionFns
{
  public static Function<String, String> toFunction(final ExtractionFn extractionFn)
  {
    return new Function<String, String>()
    {
      @Nullable
      @Override
      public String apply(@Nullable String input)
      {
        return extractionFn.apply(input);
      }
    };
  }

  public static Function<String, String>[] toFunctionsWithNull(ExtractionFn[] extractionFns, int length)
  {
    if (extractionFns == null) {
      Function<String, String>[] functions = new Function[length];
      Arrays.fill(functions, Functions.identity());
      return functions;
    }
    return toFunctions(extractionFns);
  }

  public static Function<String, String>[] toFunctions(ExtractionFn[] extractionFns)
  {
    final Function[] functions = new Function[extractionFns.length];
    for (int i = 0; i < functions.length; i++) {
      functions[i] = extractionFns[i] != null ? toFunction(extractionFns[i]) : Functions.identity();
    }
    return functions;
  }

  public static Function<Object[], Object[]> toTransform(final ExtractionFn[] extractionFns)
  {
    if (extractionFns == null) {
      return Functions.identity();
    }
    final Function[] functions = toFunctions(extractionFns);
    return new Function<Object[], Object[]>()
    {
      @Override
      public Object[] apply(Object[] input)
      {
        final Object[] transformed = new Object[functions.length];
        for (int i = 0; i < functions.length; i++) {
          transformed[i] = functions[i].apply(input[i]);
        }
        return transformed;
      }
    };
  }
}
