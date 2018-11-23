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

package org.apache.druid.query.aggregation.datasketches.theta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.PostAggregatorIds;
import org.apache.druid.query.cache.CacheKeyBuilder;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 */
public class SketchConstantPostAggregator implements PostAggregator
{

  private final String name;
  private final String value;
  private final SketchHolder sketchValue;

  @JsonCreator
  public SketchConstantPostAggregator(@JsonProperty("name") String name, @JsonProperty("value") String value)
  {
    this.name = name;
    Preconditions.checkArgument(value != null && !value.isEmpty(),
        "Constant value cannot be null or empty, expecting base64 encoded sketch string");
    this.value = value;
    this.sketchValue = SketchHolder.deserialize(value);
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Collections.emptySet();
  }

  @Override
  public Comparator<Object> getComparator()
  {
    return SketchHolder.COMPARATOR;
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators)
  {
    return sketchValue;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public SketchConstantPostAggregator decorate(Map<String, AggregatorFactory> aggregators)
  {
    return this;
  }

  @JsonProperty("value")
  public SketchHolder getSketchValue()
  {
    return sketchValue;
  }

  @Override
  public String toString()
  {
    return "SketchConstantPostAggregator{name='" + name + "', value='" + value + "'}";
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
    SketchConstantPostAggregator that = (SketchConstantPostAggregator) o;
    if (!this.sketchValue.equals(that.sketchValue)) {
      return false;
    }
    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 37 * result + sketchValue.hashCode();
    return result;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(PostAggregatorIds.THETA_SKETCH_CONSTANT)
        .appendString(DigestUtils.sha1Hex(value)).build();
  }
}
