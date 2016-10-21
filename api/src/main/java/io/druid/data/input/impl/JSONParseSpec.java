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
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.druid.java.util.common.parsers.JSONPathParser;
import io.druid.java.util.common.parsers.Parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class JSONParseSpec extends ParseSpec
{
  private final ObjectMapper objectMapper;
  private final JSONPathSpec flattenSpec;
  private final Map<String, Boolean> featureSpec;

  @JsonCreator
  public JSONParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("flattenSpec") JSONPathSpec flattenSpec,
      @JsonProperty("featureSpec") Map<String, Boolean> featureSpec
  )
  {
    super(timestampSpec, dimensionsSpec);
    this.objectMapper = new ObjectMapper();
    this.flattenSpec = flattenSpec != null ? flattenSpec : new JSONPathSpec(true, null);
    this.featureSpec = (featureSpec == null) ? new HashMap<String, Boolean>() : featureSpec;
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
  public void verify(List<String> usedCols)
  {
  }

  @Override
  public Parser<String, Object> makeParser()
  {
    return new JSONPathParser(
        convertFieldSpecs(flattenSpec.getFields()),
        flattenSpec.isUseFieldDiscovery(),
        objectMapper
    );
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
  public JSONPathSpec getFlattenSpec()
  {
    return flattenSpec;
  }

  @JsonProperty
  public Map<String, Boolean> getFeatureSpec()
  {
    return featureSpec;
  }

  private List<JSONPathParser.FieldSpec> convertFieldSpecs(List<JSONPathFieldSpec> druidFieldSpecs)
  {
    List<JSONPathParser.FieldSpec> newSpecs = new ArrayList<>();
    for (JSONPathFieldSpec druidSpec : druidFieldSpecs) {
      JSONPathParser.FieldType type;
      switch (druidSpec.getType()) {
        case ROOT:
          type = JSONPathParser.FieldType.ROOT;
          break;
        case PATH:
          type = JSONPathParser.FieldType.PATH;
          break;
        default:
          throw new IllegalArgumentException("Invalid type for field " + druidSpec.getName());
      }

      JSONPathParser.FieldSpec newSpec = new JSONPathParser.FieldSpec(
          type,
          druidSpec.getName(),
          druidSpec.getExpr()
      );
      newSpecs.add(newSpec);
    }
    return newSpecs;
  }
}
