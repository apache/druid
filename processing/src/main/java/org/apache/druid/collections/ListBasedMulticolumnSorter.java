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

import org.apache.druid.java.util.common.ISE;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This sorter is suitable for sorting full data
 */
public class ListBasedMulticolumnSorter<T> implements MultiColumnSorter<T>
{

  private final Comparator<MultiColumnSorterElement<T>> comparator;
  private final List<MultiColumnSorterElement<T>> list = new ArrayList<>();

  public ListBasedMulticolumnSorter(Comparator<MultiColumnSorterElement<T>> comparator)
  {
    this.comparator = comparator;
  }

  @Override
  public void add(MultiColumnSorterElement<T> sorterElement)
  {
    list.add(sorterElement);
  }

  @Override
  public Iterator<T> drain()
  {
    try {
      list.sort(comparator);
    }
    catch (ClassCastException e) {
      throw new ISE("The sorted column cannot have different types of values.");
    }
    return list.stream().map(sorterElement -> sorterElement.getElement()).collect(Collectors.toList()).iterator();
  }

  @Override
  public Iterator<T> drain(int limit)
  {
    list.sort(comparator);
    return list.stream().limit(limit).map(sorterElement -> sorterElement.getElement()).collect(Collectors.toList()).iterator();
  }

  @Override
  public int size()
  {
    return list.size();
  }
}
