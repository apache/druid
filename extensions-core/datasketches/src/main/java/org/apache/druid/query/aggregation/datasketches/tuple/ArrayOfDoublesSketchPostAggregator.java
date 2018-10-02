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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Base class for all post aggs
 */
public abstract class ArrayOfDoublesSketchPostAggregator implements PostAggregator
{

  private final String name;

  public ArrayOfDoublesSketchPostAggregator(final String name)
  {
    this.name = Preconditions.checkNotNull(name, "name is null");
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Collections.emptySet();
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ArrayOfDoublesSketchPostAggregator)) {
      return false;
    }
    final ArrayOfDoublesSketchPostAggregator that = (ArrayOfDoublesSketchPostAggregator) o;
    return name.equals(that.getName());
  }

  @Override
  public int hashCode()
  {
    return name.hashCode();
  }

  @Override
  public PostAggregator decorate(final Map<String, AggregatorFactory> map)
  {
    return this;
  }

}
