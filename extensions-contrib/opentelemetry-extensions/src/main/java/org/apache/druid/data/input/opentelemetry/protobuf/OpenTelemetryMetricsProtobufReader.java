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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class OpenTelemetryMetricsProtobufReader implements InputEntityReader
{
  private static final Logger log = new Logger(OpenTelemetryMetricsProtobufReader.class);

  private final ByteEntity source;
  private final String metricDimension;
  private final String valueDimension;
  private final String metricAttributePrefix;
  private final String resourceAttributePrefix;
  private final DimensionsSpec dimensionsSpec;

  public OpenTelemetryMetricsProtobufReader(
      DimensionsSpec dimensionsSpec,
      ByteEntity source,
      String metricDimension,
      String valueDimension,
      String metricAttributePrefix,
      String resourceAttributePrefix
  )
  {
    this.dimensionsSpec = dimensionsSpec;
    this.source = source;
    this.metricDimension = metricDimension;
    this.valueDimension = valueDimension;
    this.metricAttributePrefix = metricAttributePrefix;
    this.resourceAttributePrefix = resourceAttributePrefix;
  }

  @Override
  public CloseableIterator<InputRow> read()
  {
    Supplier<Iterator<InputRow>> supplier = Suppliers.memoize(() -> readAsList().iterator());
    return CloseableIterators.withEmptyBaggage(new Iterator<InputRow>() {
      @Override
      public boolean hasNext()
      {
        return supplier.get().hasNext();
      }
      @Override
      public InputRow next()
      {
        return supplier.get().next();
      }
    });
  }

  List<InputRow> readAsList()
  {
    try {
      return parseMetricsData(MetricsData.parseFrom(source.getBuffer()));
    }
    catch (InvalidProtocolBufferException e) {
      throw new ParseException(null, e, "Protobuf message could not be parsed");
    }
  }

  private List<InputRow> parseMetricsData(final MetricsData metricsData)
  {
    return metricsData.getResourceMetricsList()
        .stream()
        .flatMap(resourceMetrics -> {
          Map<String, Object> resourceAttributes = resourceMetrics.getResource()
              .getAttributesList()
              .stream()
              .collect(HashMap::new,
                  (m, kv) -> {
                    Object value = parseAnyValue(kv.getValue());
                    if (value != null) {
                      m.put(resourceAttributePrefix + kv.getKey(), value);
                    }
                  },
                  HashMap::putAll);
          return resourceMetrics.getInstrumentationLibraryMetricsList()
              .stream()
              .flatMap(libraryMetrics -> libraryMetrics.getMetricsList()
                  .stream()
                  .flatMap(metric -> parseMetric(metric, resourceAttributes).stream()));
        })
        .collect(Collectors.toList());
  }

  private List<InputRow> parseMetric(Metric metric, Map<String, Object> resourceAttributes)
  {
    final List<InputRow> inputRows;
    String metricName = metric.getName();
    switch (metric.getDataCase()) {
      case SUM: {
        inputRows = new ArrayList<>(metric.getSum().getDataPointsCount());
        metric.getSum()
            .getDataPointsList()
            .forEach(dataPoint -> inputRows.add(parseNumberDataPoint(dataPoint, resourceAttributes, metricName)));
        break;
      }
      case GAUGE: {
        inputRows = new ArrayList<>(metric.getGauge().getDataPointsCount());
        metric.getGauge()
            .getDataPointsList()
            .forEach(dataPoint -> inputRows.add(parseNumberDataPoint(dataPoint, resourceAttributes, metricName)));
        break;
      }
      // TODO Support HISTOGRAM and SUMMARY metrics
      case HISTOGRAM:
      case SUMMARY:
      default:
        log.trace("Metric type {} is not supported", metric.getDataCase());
        inputRows = Collections.emptyList();

    }
    return inputRows;
  }

  private InputRow parseNumberDataPoint(NumberDataPoint dataPoint,
                                        Map<String, Object> resourceAttributes,
                                        String metricName)
  {

    int capacity = resourceAttributes.size()
          + dataPoint.getAttributesCount()
          + 2; // metric name + value columns
    Map<String, Object> event = Maps.newHashMapWithExpectedSize(capacity);
    event.put(metricDimension, metricName);

    if (dataPoint.hasAsInt()) {
      event.put(valueDimension, dataPoint.getAsInt());
    } else {
      event.put(valueDimension, dataPoint.getAsDouble());
    }

    event.putAll(resourceAttributes);
    dataPoint.getAttributesList().forEach(att -> {
      Object value = parseAnyValue(att.getValue());
      if (value != null) {
        event.put(metricAttributePrefix + att.getKey(), value);
      }
    });

    return createRow(TimeUnit.NANOSECONDS.toMillis(dataPoint.getTimeUnixNano()), event);
  }

  @Nullable
  private static Object parseAnyValue(AnyValue value)
  {
    switch (value.getValueCase()) {
      case INT_VALUE:
        return value.getIntValue();
      case BOOL_VALUE:
        return value.getBoolValue();
      case DOUBLE_VALUE:
        return value.getDoubleValue();
      case STRING_VALUE:
        return value.getStringValue();

      // TODO: Support KVLIST_VALUE, ARRAY_VALUE and BYTES_VALUE

      default:
        // VALUE_NOT_SET
        return null;
    }
  }

  InputRow createRow(long timeUnixMilli, Map<String, Object> event)
  {
    final List<String> dimensions;
    if (!dimensionsSpec.getDimensionNames().isEmpty()) {
      dimensions = dimensionsSpec.getDimensionNames();
    } else {
      dimensions = new ArrayList<>(Sets.difference(event.keySet(), dimensionsSpec.getDimensionExclusions()));
    }
    return new MapBasedInputRow(timeUnixMilli, dimensions, event);
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample()
  {
    return read().map(row -> InputRowListPlusRawValues.of(row, ((MapBasedInputRow) row).getEvent()));
  }
}
