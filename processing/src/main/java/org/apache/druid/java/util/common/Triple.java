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

package org.apache.druid.java.util.common;

import javax.annotation.Nullable;
import java.util.Objects;

public class Triple<T1, T2, T3>
{
  @Nullable
  public final T1 first;
  @Nullable
  public final T2 second;
  @Nullable
  public final T3 third;

  public Triple(@Nullable T1 first, @Nullable T2 second, @Nullable T3 third)
  {
    this.first = first;
    this.second = second;
    this.third = third;
  }

  public static <T1, T2, T3> Triple<T1, T2, T3> of(@Nullable T1 first, @Nullable T2 second, @Nullable T3 third)
  {
    return new Triple<>(first, second, third);
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
    Triple<?, ?, ?> triple = (Triple<?, ?, ?>) o;
    return Objects.equals(first, triple.first)
           && Objects.equals(second, triple.second)
           && Objects.equals(third, triple.third);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(first, second, third);
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
