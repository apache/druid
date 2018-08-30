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

/**
 */
public class Pair<T1, T2>
{

  public static <T1, T2> Pair<T1, T2> of(@Nullable T1 lhs, @Nullable T2 rhs)
  {
    return new Pair<>(lhs, rhs);
  }

  @Nullable
  public final T1 lhs;

  @Nullable
  public final T2 rhs;

  public Pair(
      @Nullable T1 lhs,
      @Nullable T2 rhs
  )
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
    if (!(o instanceof Pair)) {
      return false;
    }
    Pair pair = (Pair) o;
    return Objects.equals(lhs, pair.lhs) && Objects.equals(rhs, pair.rhs);
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
}
