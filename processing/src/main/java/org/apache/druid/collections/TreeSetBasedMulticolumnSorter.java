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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.ISE;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 *  This sorter will de duplicate key during sorting
 */
public class TreeSetBasedMulticolumnSorter<T> implements MultiColumnSorter<T>
{
  private final TreeSet<MultiColumnSorterElement<T>> treeSet;

  public TreeSetBasedMulticolumnSorter(Comparator<MultiColumnSorter.MultiColumnSorterElement<T>> comparator)
  {
    treeSet = new TreeSet<>(comparator);
  }


  @Override
  public void add(MultiColumnSorter.MultiColumnSorterElement<T> sorterElement)
  {
    try {
      treeSet.add(sorterElement);
    }
    catch (ClassCastException e) {
      throw new ISE("The sorted column cannot have different types of values.");
    }
  }

  @Override
  public Iterator<T> drainElement()
  {
    return treeSet.stream()
                  .map(sorterElement -> sorterElement.getElement())
                  .collect(Collectors.toList())
                  .iterator();
  }

  @Override
  public Iterator<ImmutableMap<T, List<Comparable>>> drainOrderByColumValues()
  {
    return treeSet.stream()
                  .map(sorterElement -> ImmutableMap.of(sorterElement.getElement(), sorterElement.getOrderByColumValues()))
                  .collect(Collectors.toList())
                  .iterator();
  }

  public Iterator<T> drain(int limit)
  {
    return treeSet.stream().limit(limit).map(sorterElement -> sorterElement.getElement()).collect(Collectors.toList()).iterator();
  }

  @Override
  public int size()
  {
    return treeSet.size();
  }
}
