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

package org.apache.druid.segment.nested;

import org.apache.druid.segment.DictionaryMergingIterator;
import org.apache.druid.segment.data.Indexed;

/**
 * Container to collect a set of sorted {@link Indexed} representing the global value dictionaries of some
 * {@link NestedDataComplexColumn}, to later use with {@link DictionaryMergingIterator}
 * to merge into a new global dictionary
 */
public class GlobalDictionarySortedCollector
{
  private final Indexed<String> sortedStrings;
  private final Indexed<Long> sortedLongs;
  private final Indexed<Double> sortedDoubles;
  private final Iterable<Object[]> sortedArrays;
  private final int arrayCount;

  public GlobalDictionarySortedCollector(
      Indexed<String> sortedStrings,
      Indexed<Long> sortedLongs,
      Indexed<Double> sortedDoubles,
      Iterable<Object[]> sortedArrays,
      int arrayCount
  )
  {
    this.sortedStrings = sortedStrings;
    this.sortedLongs = sortedLongs;
    this.sortedDoubles = sortedDoubles;
    this.sortedArrays = sortedArrays;
    this.arrayCount = arrayCount;
  }

  public Indexed<String> getSortedStrings()
  {
    return sortedStrings;
  }

  public Indexed<Long> getSortedLongs()
  {
    return sortedLongs;
  }

  public Indexed<Double> getSortedDoubles()
  {
    return sortedDoubles;
  }

  public Iterable<Object[]> getSortedArrays()
  {
    return sortedArrays;
  }

  public int getStringCardinality()
  {
    return sortedStrings.size();
  }

  public int getLongCardinality()
  {
    return sortedLongs.size();
  }

  public int getDoubleCardinality()
  {
    return sortedDoubles.size();
  }

  public int getArrayCardinality()
  {
    return arrayCount;
  }

  public boolean allNull()
  {
    for (String s : sortedStrings) {
      if (s != null) {
        return false;
      }
    }
    if (sortedLongs.size() > 0) {
      return false;
    }
    if (sortedDoubles.size() > 0) {
      return false;
    }
    return arrayCount == 0;
  }
}
