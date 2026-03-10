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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class LoggingEmitterAllowlistConfigTest
{
  @Test
  public void testUsesDefaultAllowlistWhenCustomPathIsNotProvided()
  {
    final Map<String, Object> configMap = new HashMap<>();
    configMap.put("shouldFilterMetrics", true);
    final LoggingEmitterConfig config = new ObjectMapper().convertValue(configMap, LoggingEmitterConfig.class);

    final LoggingEmitter emitter = new LoggingEmitter(config, new ObjectMapper());
    Assert.assertNotNull(emitter);
    Assert.assertTrue(emitter.getMetricNames().contains("jvm/gc/cpu"));
    Assert.assertTrue(emitter.getMetricNames().contains("query/time"));
  }

  @Test
  public void testReadsCustomAllowlistAsMetricObject() throws IOException
  {
    final Path allowlist = Files.createTempFile("allowlist-object", ".json");
    Files.writeString(allowlist, "{\"jvm/gc/cpu\": [], \"jvm/gc/count\": []}");

    final Map<String, Object> configMap = new HashMap<>();
    configMap.put("shouldFilterMetrics", true);
    configMap.put("metricSpecPath", allowlist.toAbsolutePath().toString());
    final LoggingEmitterConfig config = new ObjectMapper().convertValue(configMap, LoggingEmitterConfig.class);

    final LoggingEmitter emitter = new LoggingEmitter(config, new ObjectMapper());
    Assert.assertNotNull(emitter);
    Assert.assertEquals(2, emitter.getMetricNames().size());
    Assert.assertTrue(emitter.getMetricNames().contains("jvm/gc/cpu"));
    Assert.assertTrue(emitter.getMetricNames().contains("jvm/gc/count"));
    Assert.assertFalse(emitter.getMetricNames().contains("query/time"));
  }
}
