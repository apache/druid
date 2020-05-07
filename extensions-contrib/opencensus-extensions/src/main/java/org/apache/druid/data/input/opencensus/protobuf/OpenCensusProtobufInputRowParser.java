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
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.Point;
import io.opencensus.proto.metrics.v1.TimeSeries;
import org.apache.druid.data.input.ByteBufferInputRowParser;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class OpenCensusProtobufInputRowParser implements ByteBufferInputRowParser
{
  private static final Logger LOG = new Logger(OpenCensusProtobufInputRowParser.class);

  private static final String SEPARATOR = "-";
  private static final String DEFAULT_METRIC_DIMENSION = "name";
  private static final String VALUE = "value";
  private static final String TIMESTAMP_COLUMN = "timestamp";
  private static final String DEFAULT_RESOURCE_PREFIX = "";
  private final ParseSpec parseSpec;
  private final List<String> dimensions;

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
    this.dimensions = parseSpec.getDimensionsSpec().getDimensionNames();
    this.metricDimension = Strings.isNullOrEmpty(metricDimension) ? DEFAULT_METRIC_DIMENSION : metricDimension;
    this.metricLabelPrefix = StringUtils.nullToEmptyNonDruidDataString(metricPrefix);
    this.resourceLabelPrefix = resourcePrefix != null ? resourcePrefix : DEFAULT_RESOURCE_PREFIX;

    LOG.info("Creating Open Census Protobuf parser with spec:" + parseSpec);
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

    Metric metric;
    try {
      metric = Metric.parseFrom(ByteString.copyFrom(input));
    }
    catch (InvalidProtocolBufferException e) {
      throw new ParseException(e, "Protobuf message could not be parsed");
    }

    // Process metric descriptor labels map keys.
    List<String> descriptorLabels = metric.getMetricDescriptor().getLabelKeysList().stream()
        .map(s -> this.metricLabelPrefix + s.getKey())
        .collect(Collectors.toList());

    // Process resource labels map.
    Map<String, String> resourceLabelsMap = metric.getResource().getLabelsMap().entrySet().stream()
        .collect(Collectors.toMap(entry -> this.resourceLabelPrefix + entry.getKey(),
            Map.Entry::getValue));

    final List<String> dimensions;

    if (!this.dimensions.isEmpty()) {
      dimensions = this.dimensions;
    } else {
      Set<String> recordDimensions = new HashSet<>(descriptorLabels);

      // Add resource map key set to record dimensions.
      recordDimensions.addAll(resourceLabelsMap.keySet());

      // MetricDimension, VALUE dimensions will not be present in labelKeysList or Metric.Resource
      // map as they are derived dimensions, which get populated while parsing data for timeSeries
      // hence add them to recordDimensions.
      recordDimensions.add(metricDimension);
      recordDimensions.add(VALUE);

      dimensions = Lists.newArrayList(
          Sets.difference(recordDimensions, parseSpec.getDimensionsSpec().getDimensionExclusions())
      );
    }

    // Flatten out the OpenCensus record into druid rows.
    List<InputRow> rows = new ArrayList<>();
    for (TimeSeries ts : metric.getTimeseriesList()) {

      // Add common resourceLabels.
      Map<String, Object> labels = new HashMap<>(resourceLabelsMap);

      // Add labels to record.
      for (int i = 0; i < metric.getMetricDescriptor().getLabelKeysCount(); i++) {
        labels.put(descriptorLabels.get(i), ts.getLabelValues(i).getValue());
      }

      // One row per timeSeries point.
      for (Point point : ts.getPointsList()) {
        // Time in millis
        labels.put(TIMESTAMP_COLUMN, point.getTimestamp().getSeconds() * 1000);

        switch (point.getValueCase()) {
          case DOUBLE_VALUE:
            Map<String, Object> doubleGauge = new HashMap<>();
            doubleGauge.putAll(labels);
            doubleGauge.put(metricDimension, metric.getMetricDescriptor().getName());
            doubleGauge.put(VALUE, point.getDoubleValue());
            addDerivedMetricsRow(doubleGauge, dimensions, rows);
            break;
          case INT64_VALUE:
            HashMap<String, Object> intGauge = new HashMap<>();
            intGauge.putAll(labels);
            intGauge.put(VALUE, point.getInt64Value());
            intGauge.put(metricDimension, metric.getMetricDescriptor().getName());
            addDerivedMetricsRow(intGauge, dimensions, rows);
            break;
          case SUMMARY_VALUE:
            // count
            Map<String, Object> summaryCount = new HashMap<>();
            summaryCount.putAll(labels);
            summaryCount.put(metricDimension, metric.getMetricDescriptor().getName() + SEPARATOR + "count");
            summaryCount.put(VALUE, point.getSummaryValue().getCount().getValue());
            addDerivedMetricsRow(summaryCount, dimensions, rows);

            // sum
            Map<String, Object> summarySum = new HashMap<>();
            summarySum.putAll(labels);
            summarySum.put(metricDimension, metric.getMetricDescriptor().getName() + SEPARATOR + "sum");
            summarySum.put(VALUE, point.getSummaryValue().getSnapshot().getSum().getValue());
            addDerivedMetricsRow(summarySum, dimensions, rows);

            // TODO : Do we put percentiles into druid ?
            break;
          case DISTRIBUTION_VALUE:
            // count
            Map<String, Object> distCount = new HashMap<>();
            distCount.putAll(labels);
            distCount.put(metricDimension, metric.getMetricDescriptor().getName() + SEPARATOR + "count");
            distCount.put(VALUE, point.getDistributionValue().getCount());
            addDerivedMetricsRow(distCount, dimensions, rows);

            // sum
            Map<String, Object> distSum = new HashMap<>();
            distSum.putAll(labels);
            distSum.put(metricDimension, metric.getMetricDescriptor().getName() + SEPARATOR + "sum");
            distSum.put(VALUE, point.getDistributionValue().getSum());
            addDerivedMetricsRow(distSum, dimensions, rows);
            // TODO: How to handle buckets ?
            break;
        }
      }
    }

    return rows;
  }

  private void addDerivedMetricsRow(Map<String, Object> derivedMetrics, List<String> dimensions,
      List<InputRow> rows)
  {
    rows.add(new MapBasedInputRow(
        parseSpec.getTimestampSpec().extractTimestamp(derivedMetrics),
        dimensions,
        derivedMetrics
    ));
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
