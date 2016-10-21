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

package io.druid.java.util.common;

import com.google.common.base.Function;

import java.util.Comparator;

/**
 */
public class Pair<T1, T2>
{
  
  public static <T1, T2> Pair<T1, T2> of(T1 lhs, T2 rhs) {
    return new Pair<>(lhs, rhs);
  }
  
  public final T1 lhs;
  public final T2 rhs;

  public Pair(T1 lhs, T2 rhs)
  {
    this.lhs = lhs;
    this.rhs = rhs;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Pair pair = (Pair) o;

    if (lhs != null ? !lhs.equals(pair.lhs) : pair.lhs != null) {
      return false;
    }
    if (rhs != null ? !rhs.equals(pair.rhs) : pair.rhs != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = lhs != null ? lhs.hashCode() : 0;
    result = 31 * result + (rhs != null ? rhs.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "Pair{" +
           "lhs=" + lhs +
           ", rhs=" + rhs +
           '}';
  }

  public static <T1, T2> Function<Pair<T1, T2>, T1> lhsFn()
  {
    return new Function<Pair<T1, T2>, T1>()
    {
      @Override
      public T1 apply(Pair<T1, T2> input)
      {
        return input.lhs;
      }
    };
  }

  public static <T1, T2> Function<Pair<T1, T2>, T2> rhsFn()
  {
    return new Function<Pair<T1, T2>, T2>()
    {
      @Override
      public T2 apply(Pair<T1, T2> input)
      {
        return input.rhs;
      }
    };
  }

  public static <T1> Comparator<Pair<T1, ?>> lhsComparator(final Comparator<T1> comparator)
  {
    return new Comparator<Pair<T1, ?>>()
    {
      @Override
      public int compare(Pair<T1, ?> o1, Pair<T1, ?> o2)
      {
        return comparator.compare(o1.lhs, o2.lhs);
      }
    };
  }
}
