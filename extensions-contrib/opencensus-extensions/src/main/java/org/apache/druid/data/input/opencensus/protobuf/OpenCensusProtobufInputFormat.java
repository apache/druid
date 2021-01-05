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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.java.util.common.StringUtils;

import java.io.File;
import java.util.Objects;

public class OpenCensusProtobufInputFormat implements InputFormat
{
  private static final String DEFAULT_METRIC_DIMENSION = "name";
  private static final String DEFAULT_RESOURCE_PREFIX = "resource.";

  private final String metricDimension;
  private final String metricLabelPrefix;
  private final String resourceLabelPrefix;

  public OpenCensusProtobufInputFormat(
      @JsonProperty("metricDimension") String metricDimension,
      @JsonProperty("metricLabelPrefix") String metricLabelPrefix,
      @JsonProperty("resourceLabelPrefix") String resourceLabelPrefix
  )
  {
    this.metricDimension = metricDimension != null ? metricDimension : DEFAULT_METRIC_DIMENSION;
    this.metricLabelPrefix = StringUtils.nullToEmptyNonDruidDataString(metricLabelPrefix);
    this.resourceLabelPrefix = resourceLabelPrefix != null ? resourceLabelPrefix : DEFAULT_RESOURCE_PREFIX;
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
  {
    return new OpenCensusProtobufReader(
        inputRowSchema.getDimensionsSpec(),
        (ByteEntity) source,
        metricDimension,
        metricLabelPrefix,
        resourceLabelPrefix
    );
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
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OpenCensusProtobufInputFormat)) {
      return false;
    }
    OpenCensusProtobufInputFormat that = (OpenCensusProtobufInputFormat) o;
    return Objects.equals(metricDimension, that.metricDimension)
           && Objects.equals(metricLabelPrefix, that.metricLabelPrefix)
           && Objects.equals(resourceLabelPrefix, that.resourceLabelPrefix);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(metricDimension, metricLabelPrefix, resourceLabelPrefix);
  }
}
