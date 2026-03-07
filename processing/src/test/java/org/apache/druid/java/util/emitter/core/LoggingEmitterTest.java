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

package org.apache.druid.java.util.emitter.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.emitter.service.UnitEvent;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class LoggingEmitterTest
{
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private List<Object> serializedObjects;
  private ObjectMapper trackingMapper;

  @Before
  public void setUp()
  {
    serializedObjects = new ArrayList<>();
    trackingMapper = new ObjectMapper()
    {
      @Override
      public String writeValueAsString(Object value) throws JsonProcessingException
      {
        serializedObjects.add(value);
        return super.writeValueAsString(value);
      }
    };
  }

  private LoggingEmitter createEmitter(boolean shouldFilterMetrics, String allowedMetricsPath)
  {
    final LoggingEmitter emitter = new LoggingEmitter(
        new Logger(LoggingEmitter.class),
        LoggingEmitter.Level.WARN,
        trackingMapper,
        shouldFilterMetrics,
        allowedMetricsPath
    );
    emitter.start();
    return emitter;
  }

  @Test
  public void testEmitAllWhenFilteringDisabled()
  {
    try (LoggingEmitter emitter = createEmitter(false, null)) {
      emitter.emit(ServiceMetricEvent.builder().setMetric("query/bytes", 100).build("test", "localhost"));
      emitter.emit(ServiceMetricEvent.builder().setMetric("jvm/gc/count", 512).build("test", "localhost"));
      emitter.emit(ServiceMetricEvent.builder().setMetric("some/random/metric", 1).build("test", "localhost"));

      Assert.assertEquals(3, serializedObjects.size());
    }
  }

  @Test
  public void testFilterWithDefaultResource()
  {
    try (LoggingEmitter emitter = createEmitter(true, null)) {
      emitter.emit(ServiceMetricEvent.builder().setMetric("jvm/gc/count", 100).build("test", "localhost"));
      emitter.emit(ServiceMetricEvent.builder().setMetric("some/unlisted/metric", 1).build("test", "localhost"));

      Assert.assertEquals(1, serializedObjects.size());
    }
  }

  @Test
  public void testFilterWithCustomFilePath() throws IOException
  {
    final File allowFile = createAllowlistFile("{\"query/bytes\": [], \"segment/scan/active\": []}");
    try (LoggingEmitter emitter = createEmitter(true, allowFile.getAbsolutePath())) {
      emitter.emit(ServiceMetricEvent.builder().setMetric("query/bytes", 100).build("test", "localhost"));
      emitter.emit(ServiceMetricEvent.builder().setMetric("jvm/gc/count", 512).build("test", "localhost"));
      emitter.emit(ServiceMetricEvent.builder().setMetric("segment/scan/active", 2048).build("test", "localhost"));

      Assert.assertEquals(2, serializedObjects.size());
    }
  }

  @Test
  public void testNonMetricEventsAlwaysPassThrough() throws IOException
  {
    final File allowFile = createAllowlistFile("{\"query/bytes\": []}");
    try (LoggingEmitter emitter = createEmitter(true, allowFile.getAbsolutePath())) {
      emitter.emit(new UnitEvent("alerts", 42));

      Assert.assertEquals(1, serializedObjects.size());
    }
  }

  @Test
  public void testMissingCustomPathThrowsAnError()
  {
    Assert.assertThrows(IAE.class, () -> createEmitter(true, "/nonexistent/path/to/allowedMetrics.json"));
  }

  @Test
  public void testEmptyAllowlistBlocksAllMetrics() throws IOException
  {
    final File allowFile = createAllowlistFile("{}");
    try (LoggingEmitter emitter = createEmitter(true, allowFile.getAbsolutePath())) {
      emitter.emit(ServiceMetricEvent.builder().setMetric("query/bytes", 100).build("test", "localhost"));
      emitter.emit(new UnitEvent("alerts", 42));

      Assert.assertEquals(1, serializedObjects.size());
    }
  }

  @Test
  public void testFilterDisabledIgnoresPath() throws IOException
  {
    final File allowFile = createAllowlistFile("{\"query/bytes\": []}");
    try (LoggingEmitter emitter = createEmitter(false, allowFile.getAbsolutePath())) {
      emitter.emit(ServiceMetricEvent.builder().setMetric("query/bytes", 100).build("test", "localhost"));
      emitter.emit(ServiceMetricEvent.builder().setMetric("jvm/gc/count", 512).build("test", "localhost"));

      Assert.assertEquals(2, serializedObjects.size());
    }
  }

  private File createAllowlistFile(String jsonContent) throws IOException
  {
    final File file = tempFolder.newFile("allowedMetrics.json");
    try (Writer writer = new OutputStreamWriter(Files.newOutputStream(file.toPath()), StandardCharsets.UTF_8)) {
      writer.write(jsonContent);
    }
    return file;
  }
}
