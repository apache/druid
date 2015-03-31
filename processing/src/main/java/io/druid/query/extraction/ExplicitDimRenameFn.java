/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import io.druid.guice.annotations.Json;
import io.druid.jackson.DefaultObjectMapper;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 *
 */
public class ExplicitDimRenameFn extends DimExtractionFn
{
  @JsonIgnore
  private final ObjectMapper mapper;

  private final Map<String, String> renames;
  private final Function<String, String> extractionFunction;

  @JsonCreator
  public ExplicitDimRenameFn(
      @JsonProperty("renames") final Map<String, String> renames
  )
  {
    this.mapper = new DefaultObjectMapper();
    this.renames = renames;
    this.extractionFunction = new Function<String, String>()
    {
      @Nullable
      @Override
      public String apply(@Nullable String input)
      {
        final String retval = renames.get(input);
        return Strings.isNullOrEmpty(retval) ? input : retval;
      }
    };
  }

  private static final byte CACHE_TYPE_ID = 0x5;

  @JsonProperty("renames")
  public Map<String, String> getRenames()
  {
    return renames;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] bytes;
    try {
      bytes = mapper.writeValueAsBytes(renames);
    }
    catch (JsonProcessingException e) {
      // Should never happen. If it can't map a Map<String, String> then there's something weird
      throw Throwables.propagate(e);
    }

    return ByteBuffer
        .allocate(bytes.length + 1)
        .put(CACHE_TYPE_ID)
        .put(bytes)
        .array();
  }

  @Override
  public String apply(String value)
  {
    return this.extractionFunction.apply(value);
  }

  @Override
  public boolean preservesOrdering()
  {
    return false;
  }
}
