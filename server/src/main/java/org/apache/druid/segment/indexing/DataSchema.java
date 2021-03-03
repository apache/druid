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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.transform.TransformSpec;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


/**
 *
 */
public class DataSchema
{
  private static final Logger log = new Logger(DataSchema.class);
  private static final Pattern INVALIDCHARS = Pattern.compile("(?s).*[^\\S ].*");
  private final String dataSource;
  private final AggregatorFactory[] aggregators;
  private final GranularitySpec granularitySpec;
  private final TransformSpec transformSpec;
  private final Map<String, Object> parserMap;
  private final ObjectMapper objectMapper;

  // The below fields can be initialized lazily from parser for backward compatibility.
  private TimestampSpec timestampSpec;
  private DimensionsSpec dimensionsSpec;

  // This is used for backward compatibility
  private InputRowParser inputRowParser;

  @JsonCreator
  public DataSchema(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("timestampSpec") @Nullable TimestampSpec timestampSpec, // can be null in old task spec
      @JsonProperty("dimensionsSpec") @Nullable DimensionsSpec dimensionsSpec, // can be null in old task spec
      @JsonProperty("metricsSpec") AggregatorFactory[] aggregators,
      @JsonProperty("granularitySpec") GranularitySpec granularitySpec,
      @JsonProperty("transformSpec") TransformSpec transformSpec,
      @Deprecated @JsonProperty("parser") @Nullable Map<String, Object> parserMap,
      @JacksonInject ObjectMapper objectMapper
  )
  {
    validateDatasourceName(dataSource);
    this.dataSource = dataSource;

    this.timestampSpec = timestampSpec;
    this.aggregators = aggregators == null ? new AggregatorFactory[]{} : aggregators;
    this.dimensionsSpec = dimensionsSpec == null
                          ? null
                          : computeDimensionsSpec(
                              Preconditions.checkNotNull(timestampSpec, "timestampSpec"),
                              dimensionsSpec,
                              this.aggregators
                          );

    if (granularitySpec == null) {
      log.warn("No granularitySpec has been specified. Using UniformGranularitySpec as default.");
      this.granularitySpec = new UniformGranularitySpec(null, null, null);
    } else {
      this.granularitySpec = granularitySpec;
    }
    this.transformSpec = transformSpec == null ? TransformSpec.NONE : transformSpec;
    this.parserMap = parserMap;
    this.objectMapper = objectMapper;

    if (aggregators != null && aggregators.length != 0) {
      // validate for no duplication
      Set<String> names = new HashSet<>();
      for (AggregatorFactory factory : aggregators) {
        if (!names.add(factory.getName())) {
          throw new IAE("duplicate aggregators found with name [%s].", factory.getName());
        }
      }
    } else if (this.granularitySpec.isRollup()) {
      log.warn("No metricsSpec has been specified. Are you sure this is what you want?");
    }
  }

  @VisibleForTesting
  public DataSchema(
      String dataSource,
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      AggregatorFactory[] aggregators,
      GranularitySpec granularitySpec,
      TransformSpec transformSpec
  )
  {
    this(dataSource, timestampSpec, dimensionsSpec, aggregators, granularitySpec, transformSpec, null, null);
  }

  // old constructor for backward compatibility
  @Deprecated
  public DataSchema(
      String dataSource,
      Map<String, Object> parserMap,
      AggregatorFactory[] aggregators,
      GranularitySpec granularitySpec,
      TransformSpec transformSpec,
      ObjectMapper objectMapper
  )
  {
    this(dataSource, null, null, aggregators, granularitySpec, transformSpec, parserMap, objectMapper);
  }

  private static void validateDatasourceName(String dataSource)
  {
    IdUtils.validateId("dataSource", dataSource);
  }

  private static DimensionsSpec computeDimensionsSpec(
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      AggregatorFactory[] aggregators
  )
  {
    final Set<String> dimensionExclusions = new HashSet<>();

    final String timestampColumn = timestampSpec.getTimestampColumn();
    if (!(dimensionsSpec.hasCustomDimensions() && dimensionsSpec.getDimensionNames().contains(timestampColumn))) {
      dimensionExclusions.add(timestampColumn);
    }

    for (AggregatorFactory aggregator : aggregators) {
      dimensionExclusions.addAll(aggregator.requiredFields());
      dimensionExclusions.add(aggregator.getName());
    }

    final Set<String> metSet = Arrays.stream(aggregators).map(AggregatorFactory::getName).collect(Collectors.toSet());
    final Set<String> dimSet = new HashSet<>(dimensionsSpec.getDimensionNames());
    final Set<String> overlap = Sets.intersection(metSet, dimSet);
    if (!overlap.isEmpty()) {
      throw new IAE(
          "Cannot have overlapping dimensions and metrics of the same name. Please change the name of the metric. Overlap: %s",
          overlap
      );
    }

    return dimensionsSpec.withDimensionExclusions(Sets.difference(dimensionExclusions, dimSet));
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @Nullable
  @JsonProperty("timestampSpec")
  private TimestampSpec getGivenTimestampSpec()
  {
    return timestampSpec;
  }

  public TimestampSpec getTimestampSpec()
  {
    if (timestampSpec == null) {
      timestampSpec = Preconditions.checkNotNull(getParser(), "inputRowParser").getParseSpec().getTimestampSpec();
    }
    return timestampSpec;
  }

  @Nullable
  @JsonProperty("dimensionsSpec")
  private DimensionsSpec getGivenDimensionsSpec()
  {
    return dimensionsSpec;
  }

  public DimensionsSpec getDimensionsSpec()
  {
    if (dimensionsSpec == null) {
      dimensionsSpec = computeDimensionsSpec(
          getTimestampSpec(),
          Preconditions.checkNotNull(getParser(), "inputRowParser").getParseSpec().getDimensionsSpec(),
          aggregators
      );
    }
    return dimensionsSpec;
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

  @JsonProperty
  public TransformSpec getTransformSpec()
  {
    return transformSpec;
  }

  @Deprecated
  @JsonProperty("parser")
  @Nullable
  @JsonInclude(Include.NON_NULL)
  public Map<String, Object> getParserMap()
  {
    return parserMap;
  }

  @Nullable
  public InputRowParser getParser()
  {
    if (inputRowParser == null) {
      if (parserMap == null) {
        return null;
      }
      //noinspection unchecked
      inputRowParser = transformSpec.decorate(objectMapper.convertValue(this.parserMap, InputRowParser.class));
      ParseSpec parseSpec = inputRowParser.getParseSpec();
      parseSpec = parseSpec.withDimensionsSpec(
          computeDimensionsSpec(parseSpec.getTimestampSpec(), parseSpec.getDimensionsSpec(), aggregators)
      );
      if (timestampSpec != null) {
        parseSpec = parseSpec.withTimestampSpec(timestampSpec);
      }
      if (dimensionsSpec != null) {
        parseSpec = parseSpec.withDimensionsSpec(dimensionsSpec);
      }
      inputRowParser = inputRowParser.withParseSpec(parseSpec);
    }
    return inputRowParser;
  }

  public DataSchema withGranularitySpec(GranularitySpec granularitySpec)
  {
    return new DataSchema(
        dataSource,
        timestampSpec,
        dimensionsSpec,
        aggregators,
        granularitySpec,
        transformSpec,
        parserMap,
        objectMapper
    );
  }

  public DataSchema withTransformSpec(TransformSpec transformSpec)
  {
    return new DataSchema(
        dataSource,
        timestampSpec,
        dimensionsSpec,
        aggregators,
        granularitySpec,
        transformSpec,
        parserMap,
        objectMapper
    );
  }

  public DataSchema withDimensionsSpec(DimensionsSpec dimensionsSpec)
  {
    return new DataSchema(
        dataSource,
        timestampSpec,
        dimensionsSpec,
        aggregators,
        granularitySpec,
        transformSpec,
        parserMap,
        objectMapper
    );
  }

  @Override
  public String toString()
  {
    return "DataSchema{" +
           "dataSource='" + dataSource + '\'' +
           ", aggregators=" + Arrays.toString(aggregators) +
           ", granularitySpec=" + granularitySpec +
           ", transformSpec=" + transformSpec +
           ", parserMap=" + parserMap +
           ", timestampSpec=" + timestampSpec +
           ", dimensionsSpec=" + dimensionsSpec +
           ", inputRowParser=" + inputRowParser +
           '}';
  }
}
