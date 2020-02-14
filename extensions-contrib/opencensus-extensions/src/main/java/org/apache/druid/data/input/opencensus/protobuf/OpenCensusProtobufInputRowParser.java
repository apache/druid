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
import com.google.common.collect.ImmutableMap;
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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OpenCensusProtobufInputRowParser implements ByteBufferInputRowParser
{
  private static final Logger LOG = new Logger(OpenCensusProtobufInputRowParser.class);

  private static final String SEPARATOR = "-";
  public static final String NAME = "name";
  public static final String VALUE = "value";
  public static final String TIMESTAMP_COLUMN = "timestamp";
  private final ParseSpec parseSpec;
  private final List<String> dimensions;

  @JsonCreator
  public OpenCensusProtobufInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec
  )
  {
    this.parseSpec = parseSpec;
    this.dimensions = parseSpec.getDimensionsSpec().getDimensionNames();
    LOG.info("Creating Open Census Protobuf parser with spec:" + parseSpec);
  }

  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public OpenCensusProtobufInputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new OpenCensusProtobufInputRowParser(parseSpec);
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

    final List<String> dimensions;

    if (!this.dimensions.isEmpty()) {
      dimensions = this.dimensions;
    } else {
      Set<String> recordDimensions = metric.getMetricDescriptor().getLabelKeysList().stream()
          .map(s -> s.getKey())
          .collect(Collectors.toSet());

      // Add resource map key set to record dimensions. Do not check for null behavior of
      // getResource() and getLabelsMap() as default instance or empty map will be returned
      // respectively. Hence it's safe to call methods directly without null check.
      recordDimensions.addAll(metric.getResource().getLabelsMap().keySet());

      // NAME, VALUE dimensions will not be present in labelKeysList or Metric.Resource map as they
      // are derived dimensions, which get populated while parsing data for timeSeries hence add
      // them to recordDimensions.
      recordDimensions.add(NAME);
      recordDimensions.add(VALUE);

      dimensions = Lists.newArrayList(
          Sets.difference(recordDimensions, parseSpec.getDimensionsSpec().getDimensionExclusions())
      );
    }

    // Create immutable resourceLabels which are common to all points in timeSeries. It's safe to
    // call getResource() and getLabelsMap() methods without null check.
    Map<String, String> resourceLabels = ImmutableMap.copyOf(metric.getResource().getLabelsMap());

    // Flatten out the OpenCensus record into druid rows.
    List<InputRow> rows = new ArrayList<>();
    for (TimeSeries ts : metric.getTimeseriesList()) {

      // Add common resourceLabels.
      Map<String, Object> labels = new HashMap<>(resourceLabels);

      // Add labels to record.
      for (int i = 0; i < metric.getMetricDescriptor().getLabelKeysCount(); i++) {
        labels.put(metric.getMetricDescriptor().getLabelKeys(i).getKey(), ts.getLabelValues(i).getValue());
      }

      // One row per timeSeries point.
      for (Point point : ts.getPointsList()) {
        // Time in millis
        labels.put(TIMESTAMP_COLUMN, point.getTimestamp().getSeconds() * 1000);

        List<Map<String, Object>> derivedMetricsList = new ArrayList<>();
        switch (point.getValueCase()) {
          case DOUBLE_VALUE:
            Map<String, Object> doubleGauge = new HashMap<>();
            doubleGauge.putAll(labels);
            doubleGauge.put(NAME, metric.getMetricDescriptor().getName());
            doubleGauge.put(VALUE, point.getDoubleValue());
            derivedMetricsList.add(doubleGauge);
            break;
          case INT64_VALUE:
            HashMap<String, Object> intGauge = new HashMap<>();
            intGauge.putAll(labels);
            intGauge.put(VALUE, point.getInt64Value());
            intGauge.put(NAME, metric.getMetricDescriptor().getName());
            derivedMetricsList.add(intGauge);
            break;
          case SUMMARY_VALUE:
            // count
            Map<String, Object> summaryCount = new HashMap<>();
            summaryCount.putAll(labels);
            summaryCount.put(NAME, metric.getMetricDescriptor().getName() + SEPARATOR + "count");
            summaryCount.put(VALUE, point.getSummaryValue().getCount().getValue());
            derivedMetricsList.add(summaryCount);

            // sum
            Map<String, Object> summarySum = new HashMap<>();
            summarySum.putAll(labels);
            summarySum.put(NAME, metric.getMetricDescriptor().getName() + SEPARATOR + "sum");
            summarySum.put(VALUE, point.getSummaryValue().getSnapshot().getSum().getValue());
            derivedMetricsList.add(summarySum);

            // TODO : Do we put percentiles into druid ?
            break;
          case DISTRIBUTION_VALUE:
            // count
            Map<String, Object> distCount = new HashMap<>();
            distCount.put(NAME, metric.getMetricDescriptor().getName() + SEPARATOR + "count");
            distCount.put(VALUE, point.getDistributionValue().getCount());
            derivedMetricsList.add(distCount);

            // sum
            Map<String, Object> distSum = new HashMap<>();
            distSum.put(NAME, metric.getMetricDescriptor().getName() + SEPARATOR + "sum");
            distSum.put(VALUE, point.getDistributionValue().getSum());
            derivedMetricsList.add(distSum);
            // TODO: How to handle buckets ?
            break;
        }

        // Add druid rows based on derivedMetrics list.
        for (Map<String, Object> derivedMetrics : derivedMetricsList) {
          rows.add(new MapBasedInputRow(
              parseSpec.getTimestampSpec().extractTimestamp(derivedMetrics),
              dimensions,
              derivedMetrics
          ));
        }
      }
    }

    return rows;
  }

}
