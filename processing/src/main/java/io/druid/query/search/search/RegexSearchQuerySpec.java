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

package io.druid.query.search.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.regex.Pattern;


public class RegexSearchQuerySpec implements SearchQuerySpec
{

  private static final byte CACHE_TYPE_ID = 0x3;

  private final String pattern;
  private final Pattern compiled;

  @JsonCreator
  public RegexSearchQuerySpec(
      @JsonProperty("pattern") String pattern
  )
  {
    this.pattern = Preconditions.checkNotNull(pattern, "pattern should not be null");
    compiled = Pattern.compile(pattern);
  }

  @JsonProperty
  public String getPattern()
  {
    return pattern;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RegexSearchQuerySpec)) {
      return false;
    }

    RegexSearchQuerySpec that = (RegexSearchQuerySpec) o;

    return pattern.equals(that.pattern);

  }

  @Override
  public int hashCode()
  {
    return pattern.hashCode();
  }

  @Override
  public boolean accept(@Nullable String dimVal)
  {
    if (dimVal == null) {
      return false;
    }

    return compiled.matcher(dimVal).find();
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] patternBytes = StringUtils.toUtf8(pattern);

    return ByteBuffer.allocate(1 + patternBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(patternBytes)
                     .array();
  }

  @Override
  public String toString()
  {
    return "RegexSearchQuerySpec{" +
           "pattern=" + pattern + "}";
  }

}
