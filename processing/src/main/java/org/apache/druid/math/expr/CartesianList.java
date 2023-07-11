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

package org.apache.druid.math.expr;

import com.google.common.base.Preconditions;
import com.google.common.math.IntMath;

import javax.annotation.Nullable;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.RandomAccess;

/**
 * {@link CartesianList} computes the cartesian product of n lists. It is adapted from and is *nearly* identical to one
 * Guava CartesianList which comes from a version from "the future" that we don't yet have, with the key difference that
 * it is not {@link com.google.common.collect.ImmutableList} based, so it can hold null values to be compatible with the
 * evaluation and handling of cartesian products of expression arrays with null elements, e.g. ['a', 'b', null]
  */

public final class CartesianList<E> extends AbstractList<List<E>> implements RandomAccess
{
  private final transient List<List<? extends E>> axes;
  private final transient int[] axesSizeProduct;

  public static <E> List<List<E>> create(List<? extends List<? extends E>> lists)
  {
    List<List<? extends E>> axesBuilder = new ArrayList<>(lists.size());
    for (List<? extends E> list : lists) {
      if (list.isEmpty()) {
        return Collections.emptyList();
      }
      axesBuilder.add(new ArrayList<>(list));
    }
    return new CartesianList<E>(axesBuilder);
  }

  CartesianList(List<List<? extends E>> axes)
  {
    this.axes = axes;
    int[] axesSizeProduct = new int[axes.size() + 1];
    axesSizeProduct[axes.size()] = 1;
    try {
      for (int i = axes.size() - 1; i >= 0; i--) {
        axesSizeProduct[i] = IntMath.checkedMultiply(axesSizeProduct[i + 1], axes.get(i).size());
      }
    }
    catch (ArithmeticException e) {
      throw new IllegalArgumentException(
          "Cartesian product too large; must have size at most Integer.MAX_VALUE");
    }
    this.axesSizeProduct = axesSizeProduct;
  }

  private int getAxisIndexForProductIndex(int index, int axis)
  {
    return (index / axesSizeProduct[axis + 1]) % axes.get(axis).size();
  }

  @Override
  public int indexOf(Object o)
  {
    if (!(o instanceof List)) {
      return -1;
    }
    List<?> list = (List<?>) o;
    if (list.size() != axes.size()) {
      return -1;
    }
    ListIterator<?> itr = list.listIterator();
    int computedIndex = 0;
    while (itr.hasNext()) {
      int axisIndex = itr.nextIndex();
      int elemIndex = axes.get(axisIndex).indexOf(itr.next());
      if (elemIndex == -1) {
        return -1;
      }
      computedIndex += elemIndex * axesSizeProduct[axisIndex + 1];
    }
    return computedIndex;
  }

  @Override
  public List<E> get(final int index)
  {
    Preconditions.checkElementIndex(index, size());
    return new AbstractList<E>()
    {
      @Override
      public int size()
      {
        return axes.size();
      }

      @Override
      public E get(int axis)
      {
        Preconditions.checkElementIndex(axis, size());
        int axisIndex = getAxisIndexForProductIndex(index, axis);
        return axes.get(axis).get(axisIndex);
      }
    };
  }

  @Override
  public int size()
  {
    return axesSizeProduct[0];
  }

  @Override
  public boolean contains(@Nullable Object o)
  {
    return indexOf(o) != -1;
  }
}
