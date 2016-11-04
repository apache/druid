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

package io.druid.segment.indexing;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

/**
 */
public class DataSchema
{
  private static final Logger log = new Logger(DataSchema.class);

  private final String dataSource;
  private final Map<String, Object> parser;
  private final AggregatorFactory[] aggregators;
  private final GranularitySpec granularitySpec;

  private final ObjectMapper jsonMapper;

  @JsonCreator
  public DataSchema(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("parser") Map<String, Object> parser,
      @JsonProperty("metricsSpec") AggregatorFactory[] aggregators,
      @JsonProperty("granularitySpec") GranularitySpec granularitySpec,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "null ObjectMapper.");
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource cannot be null. Please provide a dataSource.");
    this.parser = parser;

    if (aggregators.length == 0) {
      log.warn("No metricsSpec has been specified. Are you sure this is what you want?");
    }
    this.aggregators = aggregators;

    if (granularitySpec == null) {
      log.warn("No granularitySpec has been specified. Using UniformGranularitySpec as default.");
      this.granularitySpec = new UniformGranularitySpec(null, null, null);
    } else {
      this.granularitySpec = granularitySpec;
    }
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty("parser")
  public Map<String, Object> getParserMap()
  {
    return parser;
  }

  @JsonIgnore
  public InputRowParser getParser()
  {
    if(parser == null) {
      log.warn("No parser has been specified");
      return null;
    }

    final InputRowParser inputRowParser = jsonMapper.convertValue(this.parser, InputRowParser.class);

    final Set<String> dimensionExclusions = Sets.newHashSet();
    for (AggregatorFactory aggregator : aggregators) {
      dimensionExclusions.addAll(aggregator.requiredFields());
      dimensionExclusions.add(aggregator.getName());
    }

    if (inputRowParser.getParseSpec() != null) {
      final DimensionsSpec dimensionsSpec = inputRowParser.getParseSpec().getDimensionsSpec();
      final TimestampSpec timestampSpec = inputRowParser.getParseSpec().getTimestampSpec();

      // exclude timestamp from dimensions by default, unless explicitly included in the list of dimensions
      if (timestampSpec != null) {
        final String timestampColumn = timestampSpec.getTimestampColumn();
        if (!(dimensionsSpec.hasCustomDimensions() && dimensionsSpec.getDimensionNames().contains(timestampColumn))) {
          dimensionExclusions.add(timestampColumn);
        }
      }
      if (dimensionsSpec != null) {
        final Set<String> metSet = Sets.newHashSet();
        for (AggregatorFactory aggregator : aggregators) {
          metSet.add(aggregator.getName());
        }
        final Set<String> dimSet = Sets.newHashSet(dimensionsSpec.getDimensionNames());
        final Set<String> overlap = Sets.intersection(metSet, dimSet);
        if (!overlap.isEmpty()) {
          throw new IAE(
              "Cannot have overlapping dimensions and metrics of the same name. Please change the name of the metric. Overlap: %s",
              overlap
          );
        }

        return inputRowParser.withParseSpec(
            inputRowParser.getParseSpec()
                  .withDimensionsSpec(
                      dimensionsSpec
                          .withDimensionExclusions(
                              Sets.difference(dimensionExclusions, dimSet)
                          )
                  )
        );
      } else {
        return inputRowParser;
      }
    } else {
      log.warn("No parseSpec in parser has been specified.");
      return inputRowParser;
    }
  }

  @JsonProperty("metricsSpec")
  public AggregatorFactory[] getAggregators()
  {
    return aggregators;
  }

  @JsonProperty
  public GranularitySpec getGranularitySpec()
  {
    return granularitySpec;
  }

  public DataSchema withGranularitySpec(GranularitySpec granularitySpec)
  {
    return new DataSchema(dataSource, parser, aggregators, granularitySpec, jsonMapper);
  }

  @Override
  public String toString()
  {
    return "DataSchema{" +
           "dataSource='" + dataSource + '\'' +
           ", parser=" + parser +
           ", aggregators=" + Arrays.toString(aggregators) +
           ", granularitySpec=" + granularitySpec +
           '}';
  }
}
