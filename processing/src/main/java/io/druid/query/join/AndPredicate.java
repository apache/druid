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

import java.util.List;
import java.util.Objects;

public class AndPredicate implements JoinPredicate
{
  private final List<JoinPredicate> predicates;

  @JsonCreator
  public AndPredicate(
      @JsonProperty("predicates") List<JoinPredicate> predicates
  )
  {
    Preconditions.checkArgument(predicates != null && !predicates.isEmpty(), "predicates");
    this.predicates = predicates;
  }

  @JsonProperty
  public List<JoinPredicate> getPredicates()
  {
    return predicates;
  }

  @Override
  public JoinPredicate accept(JoinPredicateVisitor visitor)
  {
    return visitor.visit(this);
  }

  @Override
  public PredicateType getType()
  {
    return PredicateType.AND;
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

    AndPredicate that = (AndPredicate) o;
    return predicates.equals(that.predicates);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getType(), predicates);
  }
}
