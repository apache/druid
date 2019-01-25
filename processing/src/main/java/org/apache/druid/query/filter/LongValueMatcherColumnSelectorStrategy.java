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
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;

public class LongValueMatcherColumnSelectorStrategy
    implements ValueMatcherColumnSelectorStrategy<BaseLongColumnValueSelector>
{
  @Override
  public ValueMatcher makeValueMatcher(final BaseLongColumnValueSelector selector, final String value)
  {
    final Long matchVal = DimensionHandlerUtils.convertObjectToLong(value);
    if (matchVal == null) {
      return ValueMatcher.nullValueMatcher(selector);
    }
    final long matchValLong = matchVal;
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        if (selector.isNull()) {
          return false;
        }
        return selector.getLong() == matchValLong;
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
      final BaseLongColumnValueSelector selector,
      DruidPredicateFactory predicateFactory
  )
  {
    final DruidLongPredicate predicate = predicateFactory.makeLongPredicate();
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        if (selector.isNull()) {
          return predicate.applyNull();
        }
        return predicate.applyLong(selector.getLong());
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
  public ValueGetter makeValueGetter(final BaseLongColumnValueSelector selector)
  {
    return () -> {
      if (selector.isNull()) {
        return null;
      }
      return new String[]{Long.toString(selector.getLong())};
    };
  }
}
