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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.parsers.JavaScriptParser;
import org.apache.druid.java.util.common.parsers.Parser;
import org.apache.druid.js.JavaScriptConfig;

/**
 */
public class JavaScriptParseSpec extends ParseSpec
{
  private final String function;
  private final JavaScriptConfig config;

  // This variable is lazily initialized to avoid unnecessary JavaScript compilation during JSON serde
  private JavaScriptParser parser;

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
  public Parser<String, Object> makeParser()
  {
    // JavaScript configuration should be checked when it's actually used because someone might still want Druid
    // nodes to be able to deserialize JavaScript-based objects even though JavaScript is disabled.
    Preconditions.checkState(config.isEnabled(), "JavaScript is disabled");
    parser = parser == null ? new JavaScriptParser(function) : parser;
    return parser;
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

}
