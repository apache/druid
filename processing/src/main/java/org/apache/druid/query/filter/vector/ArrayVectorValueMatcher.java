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
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nullable;

public class ArrayVectorValueMatcher implements VectorValueMatcherFactory
{
  protected final TypeSignature<ValueType> columnType;
  protected final VectorObjectSelector selector;

  public ArrayVectorValueMatcher(
      TypeSignature<ValueType> columnType,
      VectorObjectSelector selector
  )
  {
    this.columnType = columnType;
    this.selector = selector;
  }

  @Override
  public VectorValueMatcher makeMatcher(@Nullable String value)
  {
    throw new UnsupportedOperationException(
        "Vectorized matcher cannot make string matcher for ARRAY types"
    );
  }

  @Override
  public VectorValueMatcher makeMatcher(Object matchValue, ColumnType matchValueType)
  {
    throw new UnsupportedOperationException(
        "Vectorized matcher cannot make object matcher for ARRAY types"
    );
  }

  @Override
  public VectorValueMatcher makeMatcher(DruidPredicateFactory predicateFactory)
  {
    final DruidObjectPredicate<Object[]> predicate = predicateFactory.makeArrayPredicate(columnType);
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
          Object o = vector[rowNum];
          if ((o == null || o instanceof Object[])) {
            if (predicate.apply((Object[]) o).matches(includeUnknown)) {
              selection[numRows++] = rowNum;
            }
          } else if (predicate.apply(new Object[]{o}).matches(includeUnknown)) {
            selection[numRows++] = rowNum;
          }
        }

        match.setSelectionSize(numRows);
        return match;
      }
    };
  }
}
