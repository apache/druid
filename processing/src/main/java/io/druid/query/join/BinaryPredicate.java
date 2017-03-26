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

package io.druid.query.join;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public abstract class BinaryPredicate implements JoinPredicate
{
  protected final JoinPredicate left;
  protected final JoinPredicate right;

  BinaryPredicate(JoinPredicate left, JoinPredicate right)
  {
    this.left = Objects.requireNonNull(left);
    this.right = Objects.requireNonNull(right);
  }

  @JsonProperty
  public JoinPredicate getLeft()
  {
    return left;
  }

  @JsonProperty
  public JoinPredicate getRight()
  {
    return right;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == this) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BinaryPredicate that = (BinaryPredicate) o;
    return getType().equals(that.getType());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getType(), left, right);
  }
}
