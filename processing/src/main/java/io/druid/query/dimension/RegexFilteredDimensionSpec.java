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

package io.druid.query.dimension;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.druid.java.util.common.StringUtils;
import io.druid.query.filter.DimFilterUtils;
import io.druid.segment.DimensionSelector;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 */
public class RegexFilteredDimensionSpec extends BaseFilteredDimensionSpec
{

  private static final byte CACHE_TYPE_ID = 0x2;

  private final String pattern;

  private final Pattern compiledRegex;

  public RegexFilteredDimensionSpec(
      @JsonProperty("delegate") DimensionSpec delegate,
      @JsonProperty("pattern") String pattern //rows not matching the pattern will be discarded
  )
  {
    super(delegate);
    this.pattern = Preconditions.checkNotNull(pattern, "pattern must not be null");
    this.compiledRegex = Pattern.compile(pattern);
  }

  @JsonProperty
  public String getPattern()
  {
    return pattern;
  }

  @Override
  public DimensionSelector decorate(final DimensionSelector selector)
  {
    if (selector == null) {
      return selector;
    }

    int count = 0;
    final Map<Integer,Integer> forwardMapping = new HashMap<>();

    final int selectorCardinality = selector.getValueCardinality();
    if (selectorCardinality < 0) {
      throw new UnsupportedOperationException("Cannot decorate a selector with no dictionary");
    }

    for (int i = 0; i < selectorCardinality; i++) {
      if (compiledRegex.matcher(Strings.nullToEmpty(selector.lookupName(i))).matches()) {
        forwardMapping.put(i, count++);
      }
    }

    final int[] reverseMapping = new int[forwardMapping.size()];
    for (Map.Entry<Integer, Integer> e : forwardMapping.entrySet()) {
      reverseMapping[e.getValue().intValue()] = e.getKey().intValue();
    }
    return BaseFilteredDimensionSpec.decorate(selector, forwardMapping, reverseMapping);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] delegateCacheKey = delegate.getCacheKey();
    byte[] regexBytes = StringUtils.toUtf8(pattern);
    return ByteBuffer.allocate(2 + delegateCacheKey.length + regexBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(delegateCacheKey)
                     .put(DimFilterUtils.STRING_SEPARATOR)
                     .put(regexBytes)
                     .array();
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

    RegexFilteredDimensionSpec that = (RegexFilteredDimensionSpec) o;

    if (!delegate.equals(that.delegate)) {
      return false;
    }
    return pattern.equals(that.pattern);

  }

  @Override
  public int hashCode()
  {
    int result = delegate.hashCode();
    result = 31 * result + pattern.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "RegexFilteredDimensionSpec{" +
           "pattern='" + pattern + '\'' +
           '}';
  }
}
