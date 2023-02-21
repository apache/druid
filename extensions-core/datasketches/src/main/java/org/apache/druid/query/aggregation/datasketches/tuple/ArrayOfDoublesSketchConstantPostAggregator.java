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
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketches;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.data.SafeWritableMemory;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 * This post-aggregator converts a given Base64 encoded string to an ArrayOfDoublesSketch.
 * The input column contains name of post-aggregator output and base64 encoded input string.
 * The output is a deserialized {@link ArrayOfDoublesSketch} .
 */
public class ArrayOfDoublesSketchConstantPostAggregator extends ArrayOfDoublesSketchPostAggregator
{

  private final String value;
  private final ArrayOfDoublesSketch sketchValue;

  @JsonCreator
  public ArrayOfDoublesSketchConstantPostAggregator(@JsonProperty("name") String name, @JsonProperty("value") String value)
  {
    super(name);
    Preconditions.checkArgument(value != null && !value.isEmpty(),
        "Constant value cannot be null or empty, expecting base64 encoded sketch string");
    this.value = value;
    this.sketchValue = deserializeSafe(value);
  }

  private ArrayOfDoublesSketch deserializeFromBase64EncodedStringSafe(final String str)
  {
    return deserializeFromByteArraySafe(StringUtils.decodeBase64(StringUtils.toUtf8(str)));
  }

  private ArrayOfDoublesSketch deserializeFromByteArraySafe(final byte[] data)
  {
    return ArrayOfDoublesSketches.wrapSketch(SafeWritableMemory.wrap(data));
  }

  private ArrayOfDoublesSketch deserializeSafe(final Object serializedSketch)
  {
    return deserializeFromBase64EncodedStringSafe((String) serializedSketch);
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Collections.emptySet();
  }

  @Override
  public Comparator getComparator()
  {
    return Comparators.alwaysEqual();
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators)
  {
    return sketchValue;
  }

  @Override
  public ArrayOfDoublesSketchConstantPostAggregator decorate(Map<String, AggregatorFactory> aggregators)
  {
    return this;
  }

  @JsonProperty("value")
  public ArrayOfDoublesSketch getSketchValue()
  {
    return sketchValue;
  }

  @Override
  public String toString()
  {
    return "ArrayOfDoublesSketchConstantPostAggregator{name='" + this.getName() + "', value='" + value + "'}";
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
    ArrayOfDoublesSketchConstantPostAggregator that = (ArrayOfDoublesSketchConstantPostAggregator) o;
    if (!this.getSketchValue().equals(that.getSketchValue())) {
      return false;
    }
    if (this.getName() != null ? !this.getName().equals(that.getName()) : that.getName() != null) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int result = getName() != null ? getName().hashCode() : 0;
    result = 37 * result + sketchValue.hashCode();
    return result;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.ARRAY_OF_DOUBLES_SKETCH_CONSTANT_SKETCH_CACHE_TYPE_ID)
        .appendString(DigestUtils.sha1Hex(value)).build();
  }
}
