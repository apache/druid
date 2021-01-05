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

package org.apache.druid.data.input.opencensus.protobuf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import org.apache.druid.data.input.ByteBufferInputRowParser;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/**
 * use {@link OpenCensusProtobufInputFormat} instead
 */
@Deprecated
public class OpenCensusProtobufInputRowParser implements ByteBufferInputRowParser
{
  private static final Logger LOG = new Logger(OpenCensusProtobufInputRowParser.class);

  private static final String DEFAULT_METRIC_DIMENSION = "name";
  private static final String DEFAULT_RESOURCE_PREFIX = "";

  private final ParseSpec parseSpec;

  private final String metricDimension;
  private final String metricLabelPrefix;
  private final String resourceLabelPrefix;

  @JsonCreator
  public OpenCensusProtobufInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("metricDimension") String metricDimension,
      @JsonProperty("metricLabelPrefix") String metricPrefix,
      @JsonProperty("resourceLabelPrefix") String resourcePrefix
  )
  {
    this.parseSpec = parseSpec;
    this.metricDimension = Strings.isNullOrEmpty(metricDimension) ? DEFAULT_METRIC_DIMENSION : metricDimension;
    this.metricLabelPrefix = StringUtils.nullToEmptyNonDruidDataString(metricPrefix);
    this.resourceLabelPrefix = resourcePrefix != null ? resourcePrefix : DEFAULT_RESOURCE_PREFIX;

    LOG.info("Creating OpenCensus Protobuf parser with spec:" + parseSpec);
  }

  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @JsonProperty
  public String getMetricDimension()
  {
    return metricDimension;
  }

  @JsonProperty
  public String getMetricLabelPrefix()
  {
    return metricLabelPrefix;
  }

  @JsonProperty
  public String getResourceLabelPrefix()
  {
    return resourceLabelPrefix;
  }

  @Override
  public OpenCensusProtobufInputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new OpenCensusProtobufInputRowParser(
        parseSpec,
        metricDimension,
        metricLabelPrefix,
        resourceLabelPrefix);
  }

  @Override
  public List<InputRow> parseBatch(ByteBuffer input)
  {
    return new OpenCensusProtobufReader(
        parseSpec.getDimensionsSpec(),
        new ByteEntity(input),
        metricDimension,
        metricLabelPrefix,
        resourceLabelPrefix
    ).readAsList();
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OpenCensusProtobufInputRowParser)) {
      return false;
    }
    final OpenCensusProtobufInputRowParser that = (OpenCensusProtobufInputRowParser) o;
    return Objects.equals(parseSpec, that.parseSpec) &&
        Objects.equals(metricDimension, that.metricDimension) &&
        Objects.equals(metricLabelPrefix, that.metricLabelPrefix) &&
        Objects.equals(resourceLabelPrefix, that.resourceLabelPrefix);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(parseSpec, metricDimension, metricLabelPrefix, resourceLabelPrefix);
  }

}
