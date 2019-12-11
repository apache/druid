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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.java.util.common.parsers.JSONPathParser;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.Parser;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 */
public class JSONParseSpec extends NestedDataParseSpec<JSONPathSpec>
{
  private final ObjectMapper objectMapper;
  private final Map<String, Boolean> featureSpec;

  @JsonCreator
  public JSONParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("flattenSpec") JSONPathSpec flattenSpec,
      @JsonProperty("featureSpec") Map<String, Boolean> featureSpec
  )
  {
    super(timestampSpec, dimensionsSpec, flattenSpec != null ? flattenSpec : JSONPathSpec.DEFAULT);
    this.objectMapper = new ObjectMapper();
    this.featureSpec = (featureSpec == null) ? new HashMap<>() : featureSpec;
    for (Map.Entry<String, Boolean> entry : this.featureSpec.entrySet()) {
      Feature feature = Feature.valueOf(entry.getKey());
      objectMapper.configure(feature, entry.getValue());
    }
  }

  @Deprecated
  public JSONParseSpec(TimestampSpec ts, DimensionsSpec dims)
  {
    this(ts, dims, null, null);
  }

  @Override
  public Parser<String, Object> makeParser()
  {
    return new JSONPathParser(getFlattenSpec(), objectMapper);
  }

  @Override
  public InputFormat toInputFormat()
  {
    return new JsonInputFormat(getFlattenSpec(), getFeatureSpec());
  }

  @Override
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    return new JSONParseSpec(spec, getDimensionsSpec(), getFlattenSpec(), getFeatureSpec());
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new JSONParseSpec(getTimestampSpec(), spec, getFlattenSpec(), getFeatureSpec());
  }

  @JsonProperty
  public Map<String, Boolean> getFeatureSpec()
  {
    return featureSpec;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    final JSONParseSpec that = (JSONParseSpec) o;
    return Objects.equals(featureSpec, that.featureSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), featureSpec);
  }

  @Override
  public String toString()
  {
    return "JSONParseSpec{" +
           "timestampSpec=" + getTimestampSpec() +
           ", dimensionsSpec=" + getDimensionsSpec() +
           ", flattenSpec=" + getFlattenSpec() +
           ", featureSpec=" + featureSpec +
           '}';
  }
}
