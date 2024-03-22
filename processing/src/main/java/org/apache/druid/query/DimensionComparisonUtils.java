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

import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;

import java.util.Comparator;

/**
 * Utility class to compare dimensions
 */
public class DimensionComparisonUtils
{

  /**
   * Checks if the comparator is the natural comparator for the given type. Natural comparator for a type is the
   * comparator that will yield the same results as a.compareTo(b) for objects a, b belonging to that valueType. In such
   * cases, it'll be faster to directly use the comparison methods provided by Java or the library, as using the
   * StringComparator would be slow.
   * MSQ only supports natural comparators
   */
  public static boolean isNaturalComparator(final ValueType type, final StringComparator comparator)
  {
    if (StringComparators.NATURAL.equals(comparator)) {
      return true;
    }
    return ((type == ValueType.STRING && StringComparators.LEXICOGRAPHIC.equals(comparator))
            || (type.isNumeric() && StringComparators.NUMERIC.equals(comparator)))
           && !type.isArray();
  }

  /**
   * Creates a list comparator with custom comparator for the elements. This is used to support weird usecases where the
   * list dimensions can have custom comparators, and cannot be handled with
   * {@link ColumnCapabilities#getNullableStrategy()}. These queries can only be generated with native queries.
   */
  public static class ArrayComparator<T> implements Comparator<Object[]>
  {
    /**
     * Custom element comparator. The comparator should handle null types as well
     */
    private final Comparator<T> elementComparator;

    public ArrayComparator(Comparator<T> elementComparator)
    {
      this.elementComparator = elementComparator;
    }

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

  /**
   * Array comparator that converts the elements to their string representation, before comparing the values using
   * the provided {@link StringComparator}. It can be used when the user provides weird comparators for their
   * string arrays
   */
  public static class ArrayComparatorForUnnaturalStringComparator extends ArrayComparator<String>
  {
    public ArrayComparatorForUnnaturalStringComparator(StringComparator elementComparator)
    {
      super(elementComparator);
    }

    @Override
    protected String coerceElement(Object element)
    {
      return String.valueOf(element);
    }
  }
}
