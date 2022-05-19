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

package org.apache.druid.segment.indexing;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.transform.TransformSpec;

import javax.annotation.Nullable;
import java.util.Map;

public class TestModifiedDataSchema extends DataSchema
{
  private final String extra;

  @JsonCreator
  public TestModifiedDataSchema(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("metricsSpec") AggregatorFactory[] aggregators,
      @JsonProperty("granularitySpec") GranularitySpec granularitySpec,
      @JsonProperty("transformSpec") TransformSpec transformSpec,
      @JsonProperty("parser") @Nullable Map<String, Object> parserMap,
      @JacksonInject ObjectMapper objectMapper,
      @JsonProperty("extra") String extra
  )
  {
    super(
        dataSource,
        timestampSpec,
        dimensionsSpec,
        aggregators,
        granularitySpec,
        transformSpec,
        parserMap,
        objectMapper
    );
    this.extra = extra;
  }

  @JsonProperty("extra")
  public String getExtra()
  {
    return extra;
  }
}
