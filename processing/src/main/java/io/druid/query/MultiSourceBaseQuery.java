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

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import io.druid.java.util.common.guava.Sequence;

import java.util.Map;

public abstract class MultiSourceBaseQuery<T extends Comparable<T>> implements Query<T>
{
  private final boolean descending;
  private final Map<String, Object> context;

  public MultiSourceBaseQuery(
      boolean descending,
      Map<String, Object> context
  )
  {
    this.context = context == null ? Maps.newTreeMap() : context;
    this.descending = descending;
  }

  @Override
  public Sequence<T> run(QuerySegmentWalker walker, Map<String, Object> context)
  {
    return run(getDistributionTarget().getQuerySegmentSpec().lookup(this, walker), context);
  }

  @JsonProperty
  @Override
  public boolean isDescending()
  {
    return descending;
  }

  @Override
  @JsonProperty
  public Map<String, Object> getContext()
  {
    return context;
  }

  @Override
  public Ordering<T> getResultOrdering()
  {
    Ordering<T> retVal = Ordering.natural();
    return descending ? retVal.reverse() : retVal;
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

    MultiSourceBaseQuery that = (MultiSourceBaseQuery) o;
    if (descending != that.descending) {
      return false;
    }
    if (!context.equals(that.context)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int result = descending ? 1 : 0;
    result = 31 * result + context.hashCode();
    return result;
  }
}
