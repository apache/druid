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

package io.druid.client.indexing;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.impl.NoopInputRowParser;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.joda.time.Interval;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class ClientHadoopIndexQuery
{
  private final String id;
  private final List<String> hadoopDependencyCoordinates;
  private final ClientHadoopIngestionSpec hadoopIngestionSpec;

  public ClientHadoopIndexQuery(
      String id,
      String dataSource,
      List<Interval> intervalsToReindex,
      AggregatorFactory[] aggregators,
      List<String> dimensions,
      QueryGranularity queryGranularity,
      Map<String, Object> tuningConfig,
      List<String> hadoopDependencyCoordinates,
      ObjectMapper mapper
  )
  {
    this.id = id;
    this.hadoopDependencyCoordinates = hadoopDependencyCoordinates;

    final Map<String, Object> datasourceIngestionSpec = new HashMap<>();
    datasourceIngestionSpec.put("dataSource", dataSource);
    datasourceIngestionSpec.put("intervals", intervalsToReindex);
    if (dimensions != null) {
      datasourceIngestionSpec.put("dimensions", dimensions);
    }

    this.hadoopIngestionSpec = new ClientHadoopIngestionSpec(
        new DataSchema(
            dataSource,
            mapper.<Map<String, Object>>convertValue(
                new NoopInputRowParser(null),
                new TypeReference<Map<String, Object>>()
                {
                }
            ),
            aggregators,
            new ArbitraryGranularitySpec(
                queryGranularity == null ? QueryGranularity.NONE : queryGranularity,
                intervalsToReindex
            ),
            mapper
        ),
        new ClientHadoopIOConfig(ImmutableMap.<String, Object>of(
            "type",
            "dataSource",
            "ingestionSpec",
            datasourceIngestionSpec
        )),
        tuningConfig
    );
  }

  @JsonProperty
  public String getType()
  {
    return "index_hadoop";
  }

  @JsonProperty
  public String getId()
  {
    return id;
  }

  @JsonProperty
  public List<String> getHadoopDependencyCoordinates()
  {
    return hadoopDependencyCoordinates;
  }

  @JsonProperty("spec")
  public ClientHadoopIngestionSpec getHadoopIngestionSpec()
  {
    return hadoopIngestionSpec;
  }

}
