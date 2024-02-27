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

package org.apache.druid.query.filter.vector;

import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.filter.ConstantMatcherType;
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nullable;

/**
 * Generic object matcher on top of a {@link VectorObjectSelector}. String object selectors specifically should use
 * {@link StringObjectVectorValueMatcher} instead.
 *
 * Note that this matcher currently will always behave as a nil matcher when used with a string matcher, since there is
 * not enough machinery in place to allow custom comparison of string values against arbitrary array or complex types.
 * In other words, how do we compare a string value from a selector filter against an array or a sketch? The short
 * answer is we can't right now. Perhaps complex type serde could be extended to provide a string matcher to handle the
 * complex type case? For array types, we might consider creating a different filter (which would also be an option for
 * complex 'selectors'), or extending selector to support arrays of values.
 */
public class ObjectVectorValueMatcher implements VectorValueMatcherFactory
{
  protected final VectorObjectSelector selector;

  public ObjectVectorValueMatcher(final VectorObjectSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public VectorValueMatcher makeMatcher(@Nullable String value)
  {
    // return a traditional nil matcher, as is the custom of our people
    if (value == null) {
      return ConstantMatcherType.ALL_TRUE.asVectorMatcher(selector);
    }
    return VectorValueMatcher.allFalseObjectMatcher(selector);
  }

  @Override
  public VectorValueMatcher makeMatcher(Object matchValue, ColumnType matchValueType)
  {
    if (matchValue == null) {
      return ConstantMatcherType.ALL_TRUE.asVectorMatcher(selector);
    }
    return VectorValueMatcher.allFalseObjectMatcher(selector);
  }

  @Override
  public VectorValueMatcher makeMatcher(DruidPredicateFactory predicateFactory)
  {
    final DruidObjectPredicate<Object> predicate = predicateFactory.makeObjectPredicate();

    return new BaseVectorValueMatcher(selector)
    {
      final VectorMatch match = VectorMatch.wrap(new int[selector.getMaxVectorSize()]);

      @Override
      public ReadableVectorMatch match(final ReadableVectorMatch mask, boolean includeUnknown)
      {
        final Object[] vector = selector.getObjectVector();
        final int[] selection = match.getSelection();

        int numRows = 0;

        for (int i = 0; i < mask.getSelectionSize(); i++) {
          final int rowNum = mask.getSelection()[i];
          final Object o = vector[rowNum];
          if (predicate.apply(o).matches(includeUnknown)) {
            selection[numRows++] = rowNum;
          }
        }

        match.setSelectionSize(numRows);
        return match;
      }
    };
  }
}
