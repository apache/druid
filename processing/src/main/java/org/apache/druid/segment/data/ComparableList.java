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

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;

import java.util.List;


public class ComparableList<T extends Comparable> implements Comparable<ComparableList>
{

  private final List<T> delegate;

  public ComparableList(List<T> input)
  {
    Preconditions.checkArgument(
        input != null,
        "Input cannot be null for %s",
        ComparableList.class.getName()
    );
    this.delegate = input;
  }

  @JsonValue
  public List<T> getDelegate()
  {
    return delegate;
  }

  @Override
  public int hashCode()
  {
    return delegate.hashCode();
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

    return this.delegate.equals(((ComparableList) obj).getDelegate());
  }

  @Override
  public int compareTo(ComparableList rhs)
  {
    if (rhs == null) {
      return 1;
    }

    final int minSize = Math.min(this.getDelegate().size(), rhs.getDelegate().size());

    if (this.delegate == rhs.getDelegate()) {
      return 0;
    } else {
      for (int i = 0; i < minSize; i++) {
        final int cmp;
        T first = this.delegate.get(i);
        Object second = rhs.getDelegate().get(i);
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
      if (this.getDelegate().size() == rhs.getDelegate().size()) {
        return 0;
      } else if (this.getDelegate().size() < rhs.getDelegate().size()) {
        return -1;
      } else {
        return 1;
      }
    }
  }

  @Override
  public String toString()
  {
    return delegate.toString();
  }

  public static int compareWithComparator(
      StringComparator stringComparator,
      ComparableList lhsComparableArray,
      ComparableList rhsComparableArray
  )
  {
    final StringComparator comparator = stringComparator == null
                                        ? StringComparators.NUMERIC
                                        : stringComparator;

    if (lhsComparableArray == null && rhsComparableArray == null) {
      return 0;
    } else if (lhsComparableArray == null) {
      return -1;
    } else if (rhsComparableArray == null) {
      return 1;
    }

    List lhs = lhsComparableArray.getDelegate();
    List rhs = rhsComparableArray.getDelegate();

    int minLength = Math.min(lhs.size(), rhs.size());

    //noinspection ArrayEquality
    if (lhs == rhs) {
      return 0;
    }
    for (int i = 0; i < minLength; i++) {
      final int cmp = comparator.compare(String.valueOf(lhs.get(i)), String.valueOf(rhs.get(i)));
      if (cmp == 0) {
        continue;
      }
      return cmp;
    }
    if (lhs.size() == rhs.size()) {
      return 0;
    } else if (lhs.size() < rhs.size()) {
      return -1;
    }
    return 1;
  }
}
