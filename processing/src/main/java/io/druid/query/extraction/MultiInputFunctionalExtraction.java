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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nullable;
import java.util.List;

public abstract class MultiInputFunctionalExtraction implements ExtractionFn
{
  private final Function<List<String>, String> extractionFunction;
  private final String replaceMissingValueWith;

  public MultiInputFunctionalExtraction(
      final Function<List<String>, String> extractionFunction,
      final String replaceMissingValueWith
  )
  {
    this.replaceMissingValueWith = Strings.emptyToNull(replaceMissingValueWith);

    this.extractionFunction = new Function<List<String>, String>()
    {
      @Nullable
      @Override
      public String apply(@Nullable List<String> dimValues)
      {
        final String retval = extractionFunction.apply(dimValues);
        return Strings.isNullOrEmpty(retval) ? MultiInputFunctionalExtraction.this.replaceMissingValueWith : retval;
      }
    };
  }


  public String getReplaceMissingValueWith()
  {
    return replaceMissingValueWith;
  }

  @Override
  public String apply(Object value)
  {
    Preconditions.checkArgument(value instanceof List, "Argument should be list of Strings");
    List<String> args = (List<String>)value;

    return extractionFunction.apply(args);
  }

  @Override
  public String apply(String value)
  {
    throw new UnsupportedOperationException("Single String argument is not supported");
  }

  @Override
  public String apply(long value)
  {
    throw new UnsupportedOperationException("Single String argument is not supported");
  }

  @Override
  public boolean preservesOrdering()
  {
    return false;
  }

  @Override
  public ExtractionType getExtractionType()
  {
    return ExtractionType.MANY_TO_ONE;
  }
}
