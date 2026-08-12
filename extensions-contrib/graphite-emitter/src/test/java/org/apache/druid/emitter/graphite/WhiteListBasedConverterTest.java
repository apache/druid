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

package org.apache.druid.emitter.graphite;

import org.apache.commons.io.IOUtils;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;


public class WhiteListBasedConverterTest
{
  private static final String PREFIX = "druid";
  private static final String HOSTNAME = "testHost.yahoo.com:8080";
  private static final String SERVICE_NAME = "historical";
  private static final String DEFAULT_NAMESPACE =
      PREFIX + "." + SERVICE_NAME + "." + GraphiteEmitter.sanitize(HOSTNAME);

  private final WhiteListBasedConverter defaultWhiteListBasedConverter = new WhiteListBasedConverter(
      PREFIX,
      false,
      false,
      false,
      null,
      new DefaultObjectMapper()
  );

  @ParameterizedTest
  @CsvSource({
      "query/time, true",
      "query/node/ttfb, true",
      "query/segmentAndCache/time, true",
      "query/time/balaba, true",
      "query/tim, false",
      "segment/added/bytes, false",
      "segment/count, true",
      "segment/size, true",
      "segment/cost/raw, false",
      "coordinator/TIER_1 /cost/raw, false",
      "segment/Kost/raw, false",
      "'', false",
      "word, false",
      "coordinator, false",
      "server/, false",
      "ingest/persists/time, true",
      "jvm/mem/init, true",
      "jvm/gc/count, true"
  })
  public void testDefaultIsInWhiteList(String key, boolean expectedValue)
  {
    ServiceMetricEvent event = ServiceMetricEvent
        .builder()
        .setMetric(key, 10)
        .build(SERVICE_NAME, HOSTNAME);

    boolean isIn = defaultWhiteListBasedConverter.druidEventToGraphite(event) != null;
    Assertions.assertEquals(expectedValue, isIn);
  }

  @ParameterizedTest
  @MethodSource("parametersForTestGetPath")
  public void testGetPath(ServiceMetricEvent serviceMetricEvent, String expectedPath)
  {
    GraphiteEvent graphiteEvent = defaultWhiteListBasedConverter.druidEventToGraphite(serviceMetricEvent);
    String path = null;
    if (graphiteEvent != null) {
      path = graphiteEvent.getEventPath();
    }
    Assertions.assertEquals(expectedPath, path);
  }

  @Test
  public void testWhiteListedStringArrayDimension() throws IOException
  {
    File mapFile = File.createTempFile("testing-" + System.nanoTime(), ".json");
    mapFile.deleteOnExit();

    try (OutputStream outputStream = new FileOutputStream(mapFile)) {
      IOUtils.copyLarge(
          getClass().getResourceAsStream("/testWhiteListedStringArrayDimension.json"),
          outputStream
      );
    }

    WhiteListBasedConverter converter = new WhiteListBasedConverter(
        PREFIX,
        false,
        false,
        false,
        mapFile.getAbsolutePath(),
        new DefaultObjectMapper()
    );

    ServiceMetricEvent event = new ServiceMetricEvent.Builder()
        .setDimension("gcName", new String[]{"g1"})
        .setMetric("jvm/gc/cpu", 10)
        .build(SERVICE_NAME, HOSTNAME);

    GraphiteEvent graphiteEvent = converter.druidEventToGraphite(event);

    Assertions.assertNotNull(graphiteEvent);
    Assertions.assertEquals(DEFAULT_NAMESPACE + ".g1.jvm/gc/cpu", graphiteEvent.getEventPath());
  }

  private static Object[] parametersForTestGetPath()
  {
    return new Object[]{
        new Object[]{
            new ServiceMetricEvent.Builder().setDimension("id", "dummy_id")
                .setDimension("status", "some_status")
                .setDimension("numDimensions", "1")
                .setDimension("segment", "dummy_segment")
                .setMetric("query/segment/time/balabla/more", 10)
                .build(SERVICE_NAME, HOSTNAME),
            DEFAULT_NAMESPACE + ".query/segment/time/balabla/more"
        },
        new Object[]{
            new ServiceMetricEvent.Builder().setDimension("dataSource", "some_data_source")
                .setDimension("tier", "_default_tier")
                .setMetric("segment/max", 10)
                .build(SERVICE_NAME, HOSTNAME),
            null
        },
        new Object[]{
            new ServiceMetricEvent.Builder().setDimension("dataSource", "data-source")
                .setDimension("type", "groupBy")
                .setDimension("interval", "2013/2015")
                .setDimension("some_random_dim1", "random_dim_value1")
                .setDimension("some_random_dim2", "random_dim_value2")
                .setDimension("hasFilters", "no")
                .setDimension("duration", "P1D")
                .setDimension("remoteAddress", "194.0.90.2")
                .setDimension("id", "ID")
                .setDimension("context", "{context}")
                .setMetric("query/time", 10)
                .build(SERVICE_NAME, HOSTNAME),
            DEFAULT_NAMESPACE + ".data-source.groupBy.query/time"
        },
        new Object[]{
            new ServiceMetricEvent.Builder().setDimension("dataSource", "data-source")
                .setDimension("type", "groupBy")
                .setDimension("some_random_dim1", "random_dim_value1")
                .setMetric("ingest/persists/count", 10)
                .build(SERVICE_NAME, HOSTNAME),
            DEFAULT_NAMESPACE + ".ingest/persists/count"
        },
        new Object[]{
            new ServiceMetricEvent.Builder().setDimension("bufferpoolName", "BufferPool")
                .setDimension("type", "groupBy")
                .setDimension("some_random_dim1", "random_dim_value1")
                .setMetric("jvm/bufferpool/capacity", 10)
                .build(SERVICE_NAME, HOSTNAME),
            null
        }
    };
  }
}
