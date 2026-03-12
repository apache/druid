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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.emitter.service.UnitEvent;
import org.junit.After;
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
  private LoggingEmitter emitter;

  @Before
  public void setUp()
  {
    serializedObjects = new ArrayList<>();
    // A custom ObjectMapper that records every object passed to writeValueAsString.
    // This lets us detect which events actually reach the logging step (i.e., were NOT
    // filtered out by the allowlist). We use Level.WARN because the WARN case in emit()
    // calls writeValueAsString unconditionally (no isWarnEnabled guard), making it a
    // reliable probe for whether an event passed the allowlist check.
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
    emitter = new LoggingEmitter(
        new Logger(LoggingEmitter.class),
        LoggingEmitter.Level.WARN,
        trackingMapper,
        shouldFilterMetrics,
        allowedMetricsPath
    );
    emitter.start();
    return emitter;
  }

  @After
  public void tearDown()
  {
    if (emitter != null) {
      emitter.close();
      emitter = null;
    }
  }

  /**
   * Without filtering enabled, the emitter should log all events (backward compatibility).
   */
  @Test
  public void testEmitAllWhenFilteringDisabled()
  {
    createEmitter(false, null);

    emitter.emit(ServiceMetricEvent.builder().setMetric("query/time", 100).build("test", "localhost"));
    emitter.emit(ServiceMetricEvent.builder().setMetric("jvm/mem/used", 512).build("test", "localhost"));
    emitter.emit(ServiceMetricEvent.builder().setMetric("some/random/metric", 1).build("test", "localhost"));

    Assert.assertEquals("All events should be serialized (logged)", 3, serializedObjects.size());
  }

  /**
   * With filtering enabled and no custom path, the default classpath resource
   * (defaultMetrics.json) should be loaded. Metrics in the default list
   * are emitted; unlisted metrics are dropped.
   */
  @Test
  public void testFilterWithDefaultResource()
  {
    createEmitter(true, null);

    // "query/time" is in the default allowed metrics list
    emitter.emit(ServiceMetricEvent.builder().setMetric("query/time", 100).build("test", "localhost"));
    // "some/unlisted/metric" is NOT in the default list
    emitter.emit(ServiceMetricEvent.builder().setMetric("some/unlisted/metric", 1).build("test", "localhost"));

    Assert.assertEquals("Only the allowed metric should be serialized", 1, serializedObjects.size());
  }

  /**
   * With filtering enabled and a custom file path, only metrics from that file are emitted.
   */
  @Test
  public void testFilterWithCustomFilePath() throws IOException
  {
    final File allowFile = createAllowlistFile("{\"query/time\": [], \"query/bytes\": []}");
    createEmitter(true, allowFile.getAbsolutePath());

    emitter.emit(ServiceMetricEvent.builder().setMetric("query/time", 100).build("test", "localhost"));
    emitter.emit(ServiceMetricEvent.builder().setMetric("jvm/mem/used", 512).build("test", "localhost"));
    emitter.emit(ServiceMetricEvent.builder().setMetric("query/bytes", 2048).build("test", "localhost"));

    Assert.assertEquals("Only allowed metrics should be serialized", 2, serializedObjects.size());
  }

  /**
   * Non-metric events (like UnitEvent) should always pass through the filter,
   * even when filtering is enabled.
   */
  @Test
  public void testNonMetricEventsAlwaysPassThrough() throws IOException
  {
    final File allowFile = createAllowlistFile("{\"query/time\": []}");
    createEmitter(true, allowFile.getAbsolutePath());

    // This is NOT a ServiceMetricEvent, so it should bypass the allowlist filter
    emitter.emit(new UnitEvent("alerts", 42));

    Assert.assertEquals("Non-metric events should bypass the allowlist filter", 1, serializedObjects.size());
  }

  /**
   * When a custom path is specified but the file is missing, the emitter falls back
   * to the default classpath resource and emits successfully.
   */
  @Test
  public void testMissingCustomPathFallsBackToDefault()
  {
    createEmitter(true, "/nonexistent/path/to/allowedMetrics.json");

    // Fallback to default should allow "query/time" (in default list) and drop unlisted metrics
    emitter.emit(ServiceMetricEvent.builder().setMetric("query/time", 100).build("test", "localhost"));
    emitter.emit(ServiceMetricEvent.builder().setMetric("some/unlisted/metric", 1).build("test", "localhost"));

    Assert.assertEquals("Fallback to default should allow listed metrics only", 1, serializedObjects.size());
  }

  /**
   * An empty allowlist should block all metric events but still pass non-metric events.
   */
  @Test
  public void testEmptyAllowlistBlocksAllMetrics() throws IOException
  {
    final File allowFile = createAllowlistFile("{}");
    createEmitter(true, allowFile.getAbsolutePath());

    emitter.emit(ServiceMetricEvent.builder().setMetric("query/time", 100).build("test", "localhost"));
    emitter.emit(new UnitEvent("alerts", 42));

    Assert.assertEquals("Only non-metric event should pass through", 1, serializedObjects.size());
  }

  /**
   * When shouldFilterMetrics is false, even if an allowedMetricsPath is provided, filtering is not applied.
   */
  @Test
  public void testFilterDisabledIgnoresPath() throws IOException
  {
    final File allowFile = createAllowlistFile("{\"query/time\": []}");
    createEmitter(false, allowFile.getAbsolutePath());

    emitter.emit(ServiceMetricEvent.builder().setMetric("query/time", 100).build("test", "localhost"));
    emitter.emit(ServiceMetricEvent.builder().setMetric("jvm/mem/used", 512).build("test", "localhost"));

    Assert.assertEquals("All events should pass when filtering is disabled", 2, serializedObjects.size());
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
