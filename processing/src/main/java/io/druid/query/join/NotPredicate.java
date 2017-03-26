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

import java.util.Objects;

public class NotPredicate implements JoinPredicate
{
  private final JoinPredicate predicate;

  @JsonCreator
  public NotPredicate(
      @JsonProperty("base") JoinPredicate predicate
  )
  {
    this.predicate = predicate;
  }

  @JsonProperty("base")
  public JoinPredicate getBase()
  {
    return predicate;
  }

  @Override
  public void accept(JoinPredicateVisitor visitor)
  {
    visitor.visit(this);
  }

  @Override
  public PredicateType getType()
  {
    return PredicateType.NOT;
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

    NotPredicate that = (NotPredicate) o;
    return predicate.equals(that.predicate);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getType(), predicate);
  }
}
