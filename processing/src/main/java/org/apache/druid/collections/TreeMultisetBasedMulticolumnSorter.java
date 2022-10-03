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

package org.apache.druid.collections;

import com.google.common.collect.SortedMultiset;
import com.google.common.collect.TreeMultiset;
import org.apache.druid.java.util.common.ISE;

import java.util.Comparator;
import java.util.Iterator;
import java.util.stream.Collectors;

/**
 * TreeMultisetBaseMulticolumnSorter implements a balanced tree and sorts by user-defined comparison.
 * This sorter is suitable for sorting full data
 */
public class TreeMultisetBasedMulticolumnSorter<T> implements MultiColumnSorter<T>
{

  private final SortedMultiset<MultiColumnSorter.MultiColumnSorterElement<T>> sortedMultiset;

  public TreeMultisetBasedMulticolumnSorter(Comparator<MultiColumnSorterElement<T>> comparator)
  {
    sortedMultiset = TreeMultiset.create(comparator);
  }


  @Override
  public void add(MultiColumnSorterElement<T> sorterElement)
  {
    try {
      sortedMultiset.add(sorterElement);
    }
    catch (ClassCastException e) {
      throw new ISE("The sorted column cannot have different types of values.");
    }
  }

  @Override
  public Iterator<T> drain()
  {
    return sortedMultiset.stream()
                         .map(sorterElement -> sorterElement.getElement())
                         .collect(Collectors.toSet())
                         .iterator();
  }

  public Iterator<T> drain(int limit)
  {
    return sortedMultiset.stream().limit(limit).map(sorterElement -> sorterElement.getElement()).collect(Collectors.toList()).iterator();
  }

  @Override
  public int size()
  {
    return sortedMultiset.size();
  }
}
