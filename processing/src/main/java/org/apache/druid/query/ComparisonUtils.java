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

package org.apache.druid.query;

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.util.Comparator;

public class ComparisonUtils
{

  public static Comparator<Object> getComparatorForType(TypeSignature<ValueType> type)
  {
    switch (type.getType()) {
      case LONG:
        // The type of the object should be Long
      case DOUBLE:
        // The type of the object should be Double
      case FLOAT:
        // The type of the object should be Float
      case STRING:
        // The type of the object should be String
        return Comparator.nullsFirst((lhs, rhs) -> ((Comparable) lhs).compareTo(rhs));
      case ARRAY:
        switch (type.getElementType().getType()) {
          case STRING:
          case LONG:
          case FLOAT:
          case DOUBLE:
            // The type of the object should be List<ElementType>
            return Comparator.nullsFirst(new ListComparator(Comparators.naturalNullsFirst()));
        }
      default:
        throw DruidException.defensive("Type[%s] cannot be compared", type);
    }
  }

  /**
   * Should be a null friendly element comparator
   */
  public static class ListComparator<T> implements Comparator<Object[]>
  {
    final Comparator<T> elementComparator;

    public ListComparator(Comparator<T> elementComparator)
    {
      this.elementComparator = elementComparator;
    }


    // TODO(laksh): Test this code
    @Override
    public int compare(Object[] lhs, Object[] rhs)
    {
      //noinspection ArrayEquality
      if (lhs == rhs) {
        return 0;
      }

      if (lhs == null) {
        return -1;
      }

      if (rhs == null) {
        return 1;
      }

      final int minSize = Math.min(lhs.length, rhs.length);

      for (int index = 0; index < minSize; ++index) {
        Object lhsElement = lhs[index];
        Object rhsElement = rhs[index];
        int cmp = elementComparator.compare(coerceElement(lhsElement), coerceElement(rhsElement));
        if (cmp != 0) {
          return cmp;
        }
      }

      if (lhs.length == rhs.length) {
        return 0;
      } else if (lhs.length < rhs.length) {
        return -1;
      }
      return 1;
    }

    protected T coerceElement(Object element)
    {
      return (T) element;
    }
  }

  public static class NumericListComparatorForStringElementComparator extends ListComparator<String>
  {
    public NumericListComparatorForStringElementComparator(@Nullable StringComparator stringComparator)
    {
      super(stringComparator == null ? StringComparators.NUMERIC : stringComparator);
    }

    @Override
    protected String coerceElement(Object element)
    {
      return String.valueOf(element);
    }
  }
}
