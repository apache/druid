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

package io.druid.segment.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;

import java.util.Set;

/**
 */
public class DataSchema
{
  private final String dataSource;
  private final InputRowParser parser;
  private final AggregatorFactory[] aggregators;
  private final GranularitySpec granularitySpec;

  @JsonCreator
  public DataSchema(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("parser") InputRowParser parser,
      @JsonProperty("metricsSpec") AggregatorFactory[] aggregators,
      @JsonProperty("granularitySpec") GranularitySpec granularitySpec
  )
  {
    this.dataSource = dataSource;

    final Set<String> dimensionExclusions = Sets.newHashSet();
    for (AggregatorFactory aggregator : aggregators) {
      dimensionExclusions.addAll(aggregator.requiredFields());
    }
    if (parser != null && parser.getParseSpec() != null) {
      final DimensionsSpec dimensionsSpec = parser.getParseSpec().getDimensionsSpec();
      final TimestampSpec timestampSpec = parser.getParseSpec().getTimestampSpec();

      // exclude timestamp from dimensions by default, unless explicitly included in the list of dimensions
      if (timestampSpec != null) {
        final String timestampColumn = timestampSpec.getTimestampColumn();
        if (!(dimensionsSpec.hasCustomDimensions() && dimensionsSpec.getDimensions().contains(timestampColumn))) {
          dimensionExclusions.add(timestampColumn);
        }
      }
      if (dimensionsSpec != null) {
        this.parser = parser.withParseSpec(
            parser.getParseSpec()
                  .withDimensionsSpec(
                      dimensionsSpec
                            .withDimensionExclusions(
                                Sets.difference(dimensionExclusions,
                                                Sets.newHashSet(dimensionsSpec.getDimensions()))
                            )
                  )
        );
      } else {
        this.parser = parser;
      }
    } else {
      this.parser = parser;
    }

    this.aggregators = aggregators;
    this.granularitySpec = granularitySpec == null
                           ? new UniformGranularitySpec(null, null, null)
                           : granularitySpec;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public InputRowParser getParser()
  {
    return parser;
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
    return new DataSchema(dataSource, parser, aggregators, granularitySpec);
  }
}
