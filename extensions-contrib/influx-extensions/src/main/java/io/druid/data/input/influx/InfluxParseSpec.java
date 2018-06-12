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
package io.druid.data.input.influx;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.java.util.common.parsers.Parser;

import java.util.List;

public class InfluxParseSpec extends ParseSpec
{
  private List<String> measurementWhitelist;

  @JsonCreator
  public InfluxParseSpec(
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("whitelistMeasurements") List<String> measurementWhitelist
  )
  {
    super(
        new TimestampSpec(InfluxParser.TIMESTAMP_KEY, "millis", null),
        dimensionsSpec != null ? dimensionsSpec : new DimensionsSpec(null, null, null)
    );
    this.measurementWhitelist = measurementWhitelist;
  }

  @Override
  public Parser<String, Object> makeParser()
  {
    if (measurementWhitelist != null && measurementWhitelist.size() > 0) {
      return new InfluxParser(Sets.newHashSet(measurementWhitelist));
    } else {
      return new InfluxParser(null);
    }
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new InfluxParseSpec(spec, measurementWhitelist);
  }
}
