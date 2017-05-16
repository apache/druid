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

package io.druid.query.filter;

import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.DimensionHandlerUtils;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.filter.BooleanValueMatcher;

public class FloatValueMatcherColumnSelectorStrategy implements ValueMatcherColumnSelectorStrategy<FloatColumnSelector>
{
  @Override
  public ValueMatcher makeValueMatcher(final FloatColumnSelector selector, final String value)
  {
    final Float matchVal = DimensionHandlerUtils.convertObjectToFloat(value);
    if (matchVal == null) {
      return BooleanValueMatcher.of(false);
    }

    final int matchValIntBits = Float.floatToIntBits(matchVal);
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        return Float.floatToIntBits(selector.get()) == matchValIntBits;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", selector);
      }
    };
  }

  @Override
  public ValueMatcher makeValueMatcher(
      final FloatColumnSelector selector, DruidPredicateFactory predicateFactory
  )
  {
    final DruidFloatPredicate predicate = predicateFactory.makeFloatPredicate();
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        return predicate.applyFloat(selector.get());
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", selector);
        inspector.visit("predicate", predicate);
      }
    };
  }

  @Override
  public ValueGetter makeValueGetter(final FloatColumnSelector selector)
  {
    return new ValueGetter()
    {
      @Override
      public String[] get()
      {
        return new String[]{ Float.toString(selector.get()) };
      }
    };
  }
}
