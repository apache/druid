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
  private Groupers()
  {
    // No instantiation
  }

  public static int hash(final Object obj)
  {
    // Mask off the high bit so we can use that to determine if a bucket is used or not.
    // Also apply the same XOR transformation that j.u.HashMap applies, to improve distribution.
    final int code = obj.hashCode();
    return (code ^ (code >>> 16)) & 0x7fffffff;
  }

  public static <KeyType> Iterator<Grouper.Entry<KeyType>> mergeIterators(
      final Iterable<Iterator<Grouper.Entry<KeyType>>> iterators,
      final Comparator<KeyType> keyTypeComparator
  )
  {
    if (keyTypeComparator != null) {
      return Iterators.mergeSorted(
          iterators,
          new Comparator<Grouper.Entry<KeyType>>()
          {
            @Override
            public int compare(Grouper.Entry<KeyType> lhs, Grouper.Entry<KeyType> rhs)
            {
              return keyTypeComparator.compare(lhs.getKey(), rhs.getKey());
            }
          }
      );
    } else {
      return Iterators.concat(iterators.iterator());
    }
  }
}
