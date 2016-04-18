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

package io.druid.segment.filter;

import com.google.common.base.Function;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.ExtractionFns;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;
import org.mozilla.javascript.Context;

import java.util.Arrays;

/**
 */
public class ByRowJavaScriptFilter extends Filter.AbstractFilter
{
  private final String[] dimensions;
  private final String script;
  private final ExtractionFn[] extractionFns;

  public ByRowJavaScriptFilter(String[] dimension, String script, ExtractionFn[] extractionFns)
  {
    this.dimensions = dimension;
    this.script = script;
    this.extractionFns = extractionFns;
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    final Function<Object[], Object[]> extractor = ExtractionFns.toTransform(extractionFns);
    return factory.makeValueMatcher(
        dimensions, new JavaScriptPredicate(script)
        {
          @Override
          final boolean applyInContext(Context cx, Object[] input)
          {
            return Context.toBoolean(fnApply.call(cx, scope, scope, extractor.apply(input)));
          }
        }
    );
  }

  @Override
  public boolean supportsBitmap()
  {
    return false;
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    final Function<String, String>[] extractors = ExtractionFns.toFunctionsWithNull(extractionFns, dimensions.length);
    final JavaScriptPredicate predicate = new JavaScriptPredicate(script)
    {
      @Override
      final boolean applyInContext(Context cx, Object[] input)
      {
        return Context.toBoolean(fnApply.call(cx, scope, scope, input));
      }
    };
    final DimensionSelector[] selectors = new DimensionSelector[dimensions.length];
    for (int i = 0; i < dimensions.length; i++) {
      selectors[i] = factory.makeDimensionSelector(new DefaultDimensionSpec(dimensions[i], dimensions[i]));
      if (selectors[i] == null) {
        return BooleanValueMatcher.FALSE;
      }
    }
    return new ValueMatcher()
    {
      private final Object[] args = new Object[selectors.length];

      @Override
      public boolean matches()
      {
        for (int i = 0; i < selectors.length; i++) {
          IndexedInts row = selectors[i].getRow();
          if (row.size() == 1) {
            args[i] = extractors[i].apply(selectors[i].lookupName(row.get(0)));
          } else {
            String[] array = new String[row.size()];
            for (int j = 0; j < array.length; j++) {
              array[j] = extractors[i].apply(selectors[i].lookupName(row.get(j)));
            }
            args[i] = Context.javaToJS(array, predicate.scope);
          }
        }
        return predicate.apply(args);
      }
    };
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ByRowJavaScriptFilter that = (ByRowJavaScriptFilter) o;

    if (!Arrays.equals(dimensions, that.dimensions)) {
      return false;
    }
    if (!Arrays.equals(extractionFns, that.extractionFns)) {
      return false;
    }
    if (!script.equals(that.script)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = Arrays.hashCode(dimensions);
    result = 31 * result + script.hashCode();
    result = 31 * result + (extractionFns != null ? Arrays.hashCode(extractionFns) : 0);
    return result;
  }
}
