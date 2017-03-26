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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Objects;

public class JoinSpec implements JoinInputSpec
{
  private final JoinType joinType;
  private final JoinPredicate predicate;
  private final JoinInputSpec left;
  private final JoinInputSpec right;

  @JsonCreator
  public JoinSpec(
      @JsonProperty("joinType") JoinType joinType,
      @JsonProperty("predicate") JoinPredicate predicate,
      @JsonProperty("left") JoinInputSpec left,
      @JsonProperty("right") JoinInputSpec right
  )
  {
    this.joinType = Preconditions.checkNotNull(joinType);
    this.predicate = Preconditions.checkNotNull(predicate, "%s join requires any predicate", joinType);
    this.left = Preconditions.checkNotNull(left);
    this.right = Preconditions.checkNotNull(right);

    Preconditions.checkArgument(JoinType.INNER == joinType, "%s join type is not supported yet", joinType);
  }

  public JoinSpec accept(JoinSpecVisitor visitor)
  {
    return visitor.visit(this);
  }

  public boolean hasPredicate()
  {
    return predicate != null;
  }

  @JsonProperty
  public JoinPredicate getPredicate()
  {
    return predicate;
  }

  @JsonProperty
  public JoinType getJoinType()
  {
    return joinType;
  }

  @JsonProperty
  public JoinInputSpec getLeft() {
    return left;
  }

  @JsonProperty
  public JoinInputSpec getRight() {
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

    JoinSpec that = (JoinSpec) o;
    if (!joinType.equals(that.joinType)) {
      return false;
    }

    if (!predicate.equals(that.predicate)) {
      return false;
    }

    if (!left.equals(that.left)) {
      return false;
    }

    return right.equals(that.right);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(joinType, predicate, left, right);
  }
}
