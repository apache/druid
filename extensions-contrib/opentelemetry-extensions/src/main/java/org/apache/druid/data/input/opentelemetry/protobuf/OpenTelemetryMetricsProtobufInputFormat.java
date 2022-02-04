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

package org.apache.druid.data.input.opentelemetry.protobuf;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.java.util.common.StringUtils;

import java.io.File;
import java.util.Objects;

public class OpenTelemetryMetricsProtobufInputFormat implements InputFormat
{
  private static final String DEFAULT_METRIC_DIMENSION = "metric";
  private static final String DEFAULT_VALUE_DIMENSION = "value";
  private static final String DEFAULT_RESOURCE_PREFIX = "resource.";

  private final String metricDimension;
  private final String valueDimension;
  private final String metricAttributePrefix;
  private final String resourceAttributePrefix;

  public OpenTelemetryMetricsProtobufInputFormat(
      @JsonProperty("metricDimension") String metricDimension,
      @JsonProperty("valueDimension") String valueDimension,
      @JsonProperty("metricAttributePrefix") String metricAttributePrefix,
      @JsonProperty("resourceAttributePrefix") String resourceAttributePrefix
  )
  {
    this.metricDimension = metricDimension != null ? metricDimension : DEFAULT_METRIC_DIMENSION;
    this.valueDimension = valueDimension != null ? valueDimension : DEFAULT_VALUE_DIMENSION;
    this.metricAttributePrefix = StringUtils.nullToEmptyNonDruidDataString(metricAttributePrefix);
    this.resourceAttributePrefix = resourceAttributePrefix != null ? resourceAttributePrefix : DEFAULT_RESOURCE_PREFIX;
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
  {
    return new OpenTelemetryMetricsProtobufReader(
            inputRowSchema.getDimensionsSpec(),
            (ByteEntity) source,
            metricDimension,
            valueDimension,
            metricAttributePrefix,
            resourceAttributePrefix
    );
  }

  @JsonProperty
  public String getMetricDimension()
  {
    return metricDimension;
  }

  @JsonProperty
  public String getValueDimension()
  {
    return valueDimension;
  }

  @JsonProperty
  public String getMetricAttributePrefix()
  {
    return metricAttributePrefix;
  }

  @JsonProperty
  public String getResourceAttributePrefix()
  {
    return resourceAttributePrefix;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OpenTelemetryMetricsProtobufInputFormat)) {
      return false;
    }
    OpenTelemetryMetricsProtobufInputFormat that = (OpenTelemetryMetricsProtobufInputFormat) o;
    return Objects.equals(metricDimension, that.metricDimension)
            && Objects.equals(valueDimension, that.valueDimension)
            && Objects.equals(metricAttributePrefix, that.metricAttributePrefix)
            && Objects.equals(resourceAttributePrefix, that.resourceAttributePrefix);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(metricDimension, valueDimension, metricAttributePrefix, resourceAttributePrefix);
  }
}
