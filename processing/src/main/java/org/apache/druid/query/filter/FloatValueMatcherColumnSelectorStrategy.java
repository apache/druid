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

package org.apache.druid.query.filter;

import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;

public class FloatValueMatcherColumnSelectorStrategy
    implements ValueMatcherColumnSelectorStrategy<BaseFloatColumnValueSelector>
{
  @Override
  public ValueMatcher makeValueMatcher(final BaseFloatColumnValueSelector selector, final String value)
  {
    final Float matchVal = DimensionHandlerUtils.convertObjectToFloat(value);
    if (matchVal == null) {
      return ValueMatcher.nullValueMatcher(selector);
    }

    final int matchValIntBits = Float.floatToIntBits(matchVal);
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        if (selector.isNull()) {
          return false;
        }
        return Float.floatToIntBits(selector.getFloat()) == matchValIntBits;
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
      final BaseFloatColumnValueSelector selector,
      DruidPredicateFactory predicateFactory
  )
  {
    final DruidFloatPredicate predicate = predicateFactory.makeFloatPredicate();
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        if (selector.isNull()) {
          return predicate.applyNull();
        }
        return predicate.applyFloat(selector.getFloat());
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
  public ValueGetter makeValueGetter(final BaseFloatColumnValueSelector selector)
  {
    return () -> {
      if (selector.isNull()) {
        return null;
      }
      return new String[]{Float.toString(selector.getFloat())};
    };
  }
}
