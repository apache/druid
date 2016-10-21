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

import io.druid.java.util.common.parsers.Parser;

import java.util.List;
import java.util.Map;

/**
 */
public class TimeAndDimsParseSpec extends ParseSpec
{
  @JsonCreator
  public TimeAndDimsParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec
  )
  {
    super(
        timestampSpec != null ? timestampSpec : new TimestampSpec(null, null, null),
        dimensionsSpec != null ? dimensionsSpec : new DimensionsSpec(null, null, null)
    );
  }

  public Parser<String, Object> makeParser()
  {
    return new Parser<String, Object>()
    {
      @Override
      public Map<String, Object> parse(String input)
      {
        throw new UnsupportedOperationException("not supported");
      }

      @Override
      public void setFieldNames(Iterable<String> fieldNames)
      {
        throw new UnsupportedOperationException("not supported");
      }

      @Override
      public List<String> getFieldNames()
      {
        throw new UnsupportedOperationException("not supported");
      }
    };
  }

  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    return new TimeAndDimsParseSpec(spec, getDimensionsSpec());
  }

  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new TimeAndDimsParseSpec(getTimestampSpec(), spec);
  }
}
