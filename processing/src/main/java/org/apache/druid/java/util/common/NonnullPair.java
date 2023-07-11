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

import com.google.common.base.Preconditions;

import java.util.Objects;

public class NonnullPair<L, R>
{
  public final L lhs;
  public final R rhs;

  public NonnullPair(L lhs, R rhs)
  {
    this.lhs = Preconditions.checkNotNull(lhs, "lhs");
    this.rhs = Preconditions.checkNotNull(rhs, "rhs");
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
    NonnullPair<?, ?> that = (NonnullPair<?, ?>) o;
    return Objects.equals(lhs, that.lhs) &&
           Objects.equals(rhs, that.rhs);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(lhs, rhs);
  }

  @Override
  public String toString()
  {
    return "NonnullPair{" +
           "lhs=" + lhs +
           ", rhs=" + rhs +
           '}';
  }
}
