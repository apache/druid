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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import io.opencensus.proto.metrics.v1.LabelKey;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.Point;
import io.opencensus.proto.metrics.v1.TimeSeries;
import org.apache.druid.data.input.ByteBufferInputRowParser;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class OpenCensusProtobufInputRowParser implements ByteBufferInputRowParser
{
  private static final Logger LOG = new Logger(OpenCensusProtobufInputRowParser.class);

  private static final String SEPARATOR = "-";
  private static final String VALUE_COLUMN = "value";
  private static final String DEFAULT_METRIC_DIMENSION = "name";
  private static final String DEFAULT_RESOURCE_PREFIX = "";

  private final ParseSpec parseSpec;
  private final DimensionsSpec dimensionsSpec;

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
    this.dimensionsSpec = parseSpec.getDimensionsSpec();
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


  private interface LabelContext
  {
    void addRow(long millis, String metricName, Object value);
  }

  @Override
  public List<InputRow> parseBatch(ByteBuffer input)
  {
    final Metric metric;
    try {
      metric = Metric.parseFrom(input);
    }
    catch (InvalidProtocolBufferException e) {
      throw new ParseException(e, "Protobuf message could not be parsed");
    }

    // Process metric descriptor labels map keys.
    List<String> descriptorLabels = new ArrayList<>(metric.getMetricDescriptor().getLabelKeysCount());
    for (LabelKey s : metric.getMetricDescriptor().getLabelKeysList()) {
      descriptorLabels.add(this.metricLabelPrefix + s.getKey());
    }

    // Process resource labels map.
    Map<String, String> resourceLabelsMap = CollectionUtils.mapKeys(
        metric.getResource().getLabelsMap(),
        key -> this.resourceLabelPrefix + key
    );

    final List<String> schemaDimensions = dimensionsSpec.getDimensionNames();

    final List<String> dimensions;
    if (!schemaDimensions.isEmpty()) {
      dimensions = schemaDimensions;
    } else {
      Set<String> recordDimensions = new HashSet<>(descriptorLabels);

      // Add resource map key set to record dimensions.
      recordDimensions.addAll(resourceLabelsMap.keySet());

      // MetricDimension, VALUE dimensions will not be present in labelKeysList or Metric.Resource
      // map as they are derived dimensions, which get populated while parsing data for timeSeries
      // hence add them to recordDimensions.
      recordDimensions.add(metricDimension);
      recordDimensions.add(VALUE_COLUMN);

      dimensions = Lists.newArrayList(
          Sets.difference(recordDimensions, dimensionsSpec.getDimensionExclusions())
      );
    }

    final int capacity = resourceLabelsMap.size()
                         + descriptorLabels.size()
                         + 2; // metric name + value columns

    List<InputRow> rows = new ArrayList<>();
    for (TimeSeries ts : metric.getTimeseriesList()) {
      final LabelContext labelContext = (millis, metricName, value) -> {
        // Add common resourceLabels.
        Map<String, Object> event = new HashMap<>(capacity);
        event.putAll(resourceLabelsMap);
        // Add metric labels
        for (int i = 0; i < metric.getMetricDescriptor().getLabelKeysCount(); i++) {
          event.put(descriptorLabels.get(i), ts.getLabelValues(i).getValue());
        }
        // add metric name and value
        event.put(metricDimension, metricName);
        event.put(VALUE_COLUMN, value);
        rows.add(new MapBasedInputRow(millis, dimensions, event));
      };

      for (Point point : ts.getPointsList()) {
        addPointRows(point, metric, labelContext);
      }
    }
    return rows;
  }

  private void addPointRows(Point point, Metric metric, LabelContext labelContext)
  {
    Timestamp timestamp = point.getTimestamp();
    long millis = Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos()).toEpochMilli();
    String metricName = metric.getMetricDescriptor().getName();

    switch (point.getValueCase()) {
      case DOUBLE_VALUE:
        labelContext.addRow(millis, metricName, point.getDoubleValue());
        break;

      case INT64_VALUE:
        labelContext.addRow(millis, metricName, point.getInt64Value());
        break;

      case SUMMARY_VALUE:
        // count
        labelContext.addRow(
            millis,
            metricName + SEPARATOR + "count",
            point.getSummaryValue().getCount().getValue()
        );
        // sum
        labelContext.addRow(
            millis,
            metricName + SEPARATOR + "sum",
            point.getSummaryValue().getSnapshot().getSum().getValue()
        );
        break;

      // TODO : How to handle buckets and percentiles
      case DISTRIBUTION_VALUE:
        // count
        labelContext.addRow(millis, metricName + SEPARATOR + "count", point.getDistributionValue().getCount());
        // sum
        labelContext.addRow(
            millis,
            metricName + SEPARATOR + "sum",
            point.getDistributionValue().getSum()
        );
        break;
      default:
    }
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
