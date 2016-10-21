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

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.druid.java.util.common.ISE;
import io.druid.java.util.common.parsers.JavaScriptParser;
import io.druid.java.util.common.parsers.Parser;
import io.druid.js.JavaScriptConfig;

import java.util.List;

/**
 */
public class JavaScriptParseSpec extends ParseSpec
{
  private final String function;
  private final JavaScriptConfig config;

  @JsonCreator
  public JavaScriptParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("function") String function,
      @JacksonInject JavaScriptConfig config
  )
  {
    super(timestampSpec, dimensionsSpec);

    this.function = function;
    this.config = config;
  }

  @JsonProperty("function")
  public String getFunction()
  {
    return function;
  }

  @Override
  public void verify(List<String> usedCols)
  {
  }

  @Override
  public Parser<String, Object> makeParser()
  {
    if (config.isDisabled()) {
      throw new ISE("JavaScript is disabled");
    }

    return new JavaScriptParser(function);
  }

  @Override
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    return new JavaScriptParseSpec(spec, getDimensionsSpec(), function, config);
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new JavaScriptParseSpec(getTimestampSpec(), spec, function, config);
  }

  public ParseSpec withFunction(String fn)
  {
    return new JavaScriptParseSpec(getTimestampSpec(), getDimensionsSpec(), fn, config);
  }
}
