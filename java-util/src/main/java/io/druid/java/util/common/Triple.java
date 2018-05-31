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

public class Triple<F, S, T>
{
  public final F first;
  public final S second;
  public final T third;

  public Triple(F first, S second, T third)
  {
    this.first = first;
    this.second = second;
    this.third = third;
  }

  @Override
  public boolean equals(Object o)
  {
    if (!(o instanceof Triple)) {
      return false;
    }
    Triple<?, ?, ?> p = (Triple<?, ?, ?>) o;
    return first.equals(p.first) && second.equals(p.second) && third.equals(p.third);
  }

  private static boolean equals(Object x, Object y)
  {
    return (x == null && y == null) || (x != null && x.equals(y));
  }

  @Override
  public int hashCode()
  {
    return (first == null ? 0 : first.hashCode()) ^ (second == null ? 0 : second.hashCode()) ^ (third == null
                                                                                                ? 0
                                                                                                : third.hashCode());
  }

  public static <F, S, T> Triple<F, S, T> of(F f, S s, T t)
  {
    return new Triple<F, S, T>(f, s, t);
  }

  @Override
  public String toString()
  {
    return "Triple{" +
           "first=" + first +
           ", second=" + second +
           ", third=" + third +
           '}';
  }
}
