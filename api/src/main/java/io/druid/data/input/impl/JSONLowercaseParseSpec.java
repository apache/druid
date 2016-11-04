/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.druid.java.util.common.parsers.JSONToLowerParser;
import io.druid.java.util.common.parsers.Parser;

import java.util.List;

/**
 * This class is only here for backwards compatibility
 */
@Deprecated
public class JSONLowercaseParseSpec extends ParseSpec
{
  private final ObjectMapper objectMapper;

  @JsonCreator
  public JSONLowercaseParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec
  )
  {
    super(timestampSpec, dimensionsSpec);
    this.objectMapper = new ObjectMapper();
  }

  @Override
  public void verify(List<String> usedCols)
  {
  }

  @Override
  public Parser<String, Object> makeParser()
  {
    return new JSONToLowerParser(objectMapper, null, null);
  }

  @Override
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    return new JSONLowercaseParseSpec(spec, getDimensionsSpec());
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new JSONLowercaseParseSpec(getTimestampSpec(), spec);
  }
}
