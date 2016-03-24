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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.parsers.JavaScriptParser;
import com.metamx.common.parsers.Parser;

import java.util.List;

/**
 */
public class JavaScriptParseSpec extends ParseSpec
{
  private final String function;

  @JsonCreator
  public JavaScriptParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("function") String function
  )
  {
    super(timestampSpec, dimensionsSpec);

    this.function = function;
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
    return new JavaScriptParser(function);
  }

  @Override
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    return new JavaScriptParseSpec(spec, getDimensionsSpec(), function);
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new JavaScriptParseSpec(getTimestampSpec(), spec, function);
  }

  public ParseSpec withFunction(String fn)
  {
    return new JavaScriptParseSpec(getTimestampSpec(), getDimensionsSpec(), fn);
  }
}
