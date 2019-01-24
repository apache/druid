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
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;


public class DoubleValueMatcherColumnSelectorStrategy
    implements ValueMatcherColumnSelectorStrategy<BaseDoubleColumnValueSelector>
{
  @Override
  public ValueMatcher makeValueMatcher(final BaseDoubleColumnValueSelector selector, final String value)
  {
    final Double matchVal = DimensionHandlerUtils.convertObjectToDouble(value);
    if (matchVal == null) {
      return ValueMatcher.nullValueMatcher(selector);
    }

    final long matchValLongBits = Double.doubleToLongBits(matchVal);
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        if (selector.isNull()) {
          return false;
        }
        return Double.doubleToLongBits(selector.getDouble()) == matchValLongBits;
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
      final BaseDoubleColumnValueSelector selector,
      DruidPredicateFactory predicateFactory
  )
  {
    final DruidDoublePredicate predicate = predicateFactory.makeDoublePredicate();
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        if (selector.isNull()) {
          return predicate.applyNull();
        }
        return predicate.applyDouble(selector.getDouble());
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
  public ValueGetter makeValueGetter(final BaseDoubleColumnValueSelector selector)
  {
    return () -> {
      if (selector.isNull()) {
        return null;
      }
      return new String[]{Double.toString(selector.getDouble())};
    };
  }
}
