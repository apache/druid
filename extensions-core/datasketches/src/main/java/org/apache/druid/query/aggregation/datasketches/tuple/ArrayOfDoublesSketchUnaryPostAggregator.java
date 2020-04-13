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

package org.apache.druid.query.aggregation.datasketches.tuple;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.druid.query.aggregation.PostAggregator;

import java.util.Objects;
import java.util.Set;

/**
 * Base class for post aggs taking one sketch as input
 */
public abstract class ArrayOfDoublesSketchUnaryPostAggregator extends ArrayOfDoublesSketchPostAggregator
{

  private final PostAggregator field;
  private Set<String> dependentFields;

  @JsonCreator
  public ArrayOfDoublesSketchUnaryPostAggregator(
      final String name,
      final PostAggregator field
  )
  {
    super(name);
    this.field = Preconditions.checkNotNull(field, "field is null");
  }

  @JsonProperty
  public PostAggregator getField()
  {
    return field;
  }

  @Override
  public Set<String> getDependentFields()
  {
    if (dependentFields == null) {
      dependentFields = Sets.newHashSet(super.getDependentFields());
      dependentFields.addAll(field.getDependentFields());
    }
    return dependentFields;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (!super.equals(o)) {
      return false;
    }
    // this check is used here instead of instanceof because this is an abstract class
    // and subclasses not overriding equals should not be equal to each other
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ArrayOfDoublesSketchUnaryPostAggregator that = (ArrayOfDoublesSketchUnaryPostAggregator) o;
    return field.equals(that.getField());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), field);
  }

  @Override
  public String toString()
  {
    return this.getClass().getSimpleName() + "{" +
        "name='" + getName() + '\'' +
        ", field=" + getField() +
        "}";
  }

}
