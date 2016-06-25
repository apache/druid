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

package io.druid.query.groupby.epinephelinae;

import com.google.common.collect.Iterators;

import java.util.Comparator;
import java.util.Iterator;

public class Groupers
{
  private static final Comparator<Grouper.Entry<? extends Comparable>> ENTRY_COMPARATOR = new Comparator<Grouper.Entry<? extends Comparable>>()
  {
    @Override
    public int compare(
        final Grouper.Entry<? extends Comparable> lhs,
        final Grouper.Entry<? extends Comparable> rhs
    )
    {
      return lhs.getKey().compareTo(rhs.getKey());
    }
  };

  private Groupers()
  {
    // No instantiation
  }

  public static int hash(final Object obj)
  {
    // Mask off the high bit so we can use that to determine if a bucket is used or not.
    return obj.hashCode() & 0x7fffffff;
  }

  public static <KeyType extends Comparable<KeyType>> Iterator<Grouper.Entry<KeyType>> mergeIterators(
      final Iterable<Iterator<Grouper.Entry<KeyType>>> iterators,
      final boolean sorted
  )
  {
    if (sorted) {
      return Iterators.mergeSorted(iterators, ENTRY_COMPARATOR);
    } else {
      return Iterators.concat(iterators.iterator());
    }
  }
}
