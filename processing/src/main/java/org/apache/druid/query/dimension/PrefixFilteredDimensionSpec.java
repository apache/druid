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

package org.apache.druid.query.dimension;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.filter.DimFilterUtils;
import org.apache.druid.segment.DimensionSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 */
public class PrefixFilteredDimensionSpec extends BaseFilteredDimensionSpec
{

  private static final byte CACHE_TYPE_ID = 0x4;

  private final String prefix;

  public PrefixFilteredDimensionSpec(
      @JsonProperty("delegate") DimensionSpec delegate,
      @JsonProperty("prefix") String prefix //rows not starting with the prefix will be discarded
  )
  {
    super(delegate);
    this.prefix = Preconditions.checkNotNull(prefix, "prefix must not be null");
  }

  @JsonProperty
  public String getPrefix()
  {
    return prefix;
  }

  @Override
  public DimensionSelector decorate(final DimensionSelector selector)
  {
    if (selector == null) {
      return null;
    }

    final int selectorCardinality = selector.getValueCardinality();
    if (selectorCardinality < 0 || !selector.nameLookupPossibleInAdvance()) {
      return new PredicateFilteredDimensionSelector(
          selector,
          new Predicate<String>()
          {
            @Override
            public boolean apply(@Nullable String input)
            {
              String val = NullHandling.nullToEmptyIfNeeded(input);
              return val == null ? false : val.startsWith(prefix);
            }
          }
      );
    }

    int count = 0;
    final Int2IntOpenHashMap forwardMapping = new Int2IntOpenHashMap();
    forwardMapping.defaultReturnValue(-1);
    for (int i = 0; i < selectorCardinality; i++) {
      String val = NullHandling.nullToEmptyIfNeeded(selector.lookupName(i));
      if (val != null && val.startsWith(prefix)) {
        forwardMapping.put(i, count++);
      }
    }

    final int[] reverseMapping = new int[forwardMapping.size()];
    for (Int2IntMap.Entry e : forwardMapping.int2IntEntrySet()) {
      reverseMapping[e.getIntValue()] = e.getIntKey();
    }
    return new ForwardingFilteredDimensionSelector(selector, forwardMapping, reverseMapping);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] delegateCacheKey = delegate.getCacheKey();
    byte[] prefixBytes = StringUtils.toUtf8(prefix);
    return ByteBuffer.allocate(2 + delegateCacheKey.length + prefixBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(delegateCacheKey)
                     .put(DimFilterUtils.STRING_SEPARATOR)
                     .put(prefixBytes)
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

    PrefixFilteredDimensionSpec that = (PrefixFilteredDimensionSpec) o;

    if (!delegate.equals(that.delegate)) {
      return false;
    }
    return prefix.equals(that.prefix);
  }

  @Override
  public int hashCode()
  {
    int result = delegate.hashCode();
    result = 31 * result + prefix.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "PrefixFilteredDimensionSpec{" +
           "Prefix='" + prefix + '\'' +
           '}';
  }
}
