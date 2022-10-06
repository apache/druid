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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import java.util.Iterator;
import java.util.List;

public interface Sorter<T>
{
  /**
   * Offer an element to the sorter.If there are multiple values of different types in the same column, a CalssCastException will be thrown
   */
  void add(SorterElement<T> sorterElement);

  /**
   * Drain elements in sorted order (least first).
   */
  Iterator<T> drainElement();

  /**
   * Drain orderByColumValues in sorted order (least first).
   */
  Iterator<ImmutableMap<T, List<Comparable>>> drainOrderByColumValues();

  /**
   * Size of elements
   */
  int size();

  @VisibleForTesting
  class SorterElement<T>
  {
    private final T element;
    private final List<Comparable> orderByColumValues;

    public SorterElement(T element, List<Comparable> orderByColums)
    {
      this.element = element;
      this.orderByColumValues = orderByColums;
    }

    public T getElement()
    {
      return element;
    }

    public List<Comparable> getOrderByColumValues()
    {
      return orderByColumValues;
    }

  }

}
