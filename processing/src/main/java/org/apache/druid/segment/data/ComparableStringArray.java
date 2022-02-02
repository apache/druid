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

package org.apache.druid.segment.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;

import java.util.Arrays;

public class ComparableStringArray implements Comparable<ComparableStringArray>
{
  public static final ComparableStringArray EMPTY_ARRAY = new ComparableStringArray(new String[0]);

  final String[] delegate;
  private int hashCode;
  private boolean hashCodeComputed;

  private ComparableStringArray(String[] array)
  {
    delegate = array;
  }

  @JsonCreator
  public static ComparableStringArray of(String... array)
  {
    if (array.length == 0) {
      return EMPTY_ARRAY;
    } else {
      return new ComparableStringArray(array);
    }
  }

  @JsonValue
  public String[] getDelegate()
  {
    return delegate;
  }

  @Override
  public int hashCode()
  {
    // Check is not thread-safe, but that's fine. Even if used by multiple threads, it's ok to write these primitive
    // fields more than once.
    // As ComparableIntArray is used in hot loop caching the hashcode
    if (!hashCodeComputed) {
      hashCode = Arrays.hashCode(delegate);
      hashCodeComputed = true;
    }

    return hashCode;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    return Arrays.equals(delegate, ((ComparableStringArray) obj).getDelegate());
  }

  @Override
  public int compareTo(ComparableStringArray rhs)
  {
    // rhs.getDelegate() cannot be null
    if (rhs == null) {
      return 1;
    }
    final int minSize = Math.min(this.getDelegate().length, rhs.getDelegate().length);
    //noinspection ArrayEquality
    if (this.delegate == rhs.getDelegate()) {
      return 0;
    } else {
      for (int i = 0; i < minSize; i++) {
        final int cmp;
        String first = this.delegate[i];
        String second = rhs.getDelegate()[i];
        if (first == null && second == null) {
          cmp = 0;
        } else if (first == null) {
          cmp = -1;
        } else if (second == null) {
          cmp = 1;
        } else {
          cmp = first.compareTo(second);
        }
        if (cmp == 0) {
          continue;
        }
        return cmp;
      }
      if (this.getDelegate().length == rhs.getDelegate().length) {
        return 0;
      } else if (this.getDelegate().length < rhs.getDelegate().length) {
        return -1;
      } else {
        return 1;
      }
    }
  }

  @Override
  public String toString()
  {
    return Arrays.toString(delegate);
  }


  public static int compareWithComparator(
      StringComparator stringComparator,
      ComparableStringArray lhsComparableArray,
      ComparableStringArray rhsComparableArray
  )
  {
    final StringComparator comparator = stringComparator == null
                                        ? StringComparators.LEXICOGRAPHIC
                                        : stringComparator;
    if (lhsComparableArray == null && rhsComparableArray == null) {
      return 0;
    } else if (lhsComparableArray == null) {
      return -1;
    } else if (rhsComparableArray == null) {
      return 1;
    }

    String[] lhs = lhsComparableArray.getDelegate();
    String[] rhs = rhsComparableArray.getDelegate();

    int minLength = Math.min(lhs.length, rhs.length);

    //noinspection ArrayEquality
    if (lhs == rhs) {
      return 0;
    }
    for (int i = 0; i < minLength; i++) {
      final int cmp = comparator.compare(lhs[i], rhs[i]);
      if (cmp == 0) {
        continue;
      }
      return cmp;
    }
    if (lhs.length == rhs.length) {
      return 0;
    } else if (lhs.length < rhs.length) {
      return -1;
    }
    return 1;
  }
}
