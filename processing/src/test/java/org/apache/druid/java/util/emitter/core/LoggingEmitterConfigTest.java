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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class LoggingEmitterConfigTest
{
  @Test
  public void testDefaults()
  {
    final Properties props = new Properties();
    final ObjectMapper objectMapper = new ObjectMapper();
    final LoggingEmitterConfig config = objectMapper.convertValue(
        Emitters.makeCustomFactoryMap(props),
        LoggingEmitterConfig.class
    );
    Assertions.assertEquals(LoggingEmitter.class.getName(), config.getLoggerClass(), "getLoggerClass");
    Assertions.assertEquals("info", config.getLogLevel(), "getLogLevel");
    Assertions.assertFalse(config.shouldFilterMetrics(), "shouldFilterMetrics");
    Assertions.assertNull(config.getAllowedMetricsPath(), "getAllowedMetricsPath");
  }

  @Test
  public void testDefaultsLegacy()
  {
    final Properties props = new Properties();
    final ObjectMapper objectMapper = new ObjectMapper();
    final LoggingEmitterConfig config = objectMapper.convertValue(
        Emitters.makeLoggingMap(props),
        LoggingEmitterConfig.class
    );

    Assertions.assertEquals(LoggingEmitter.class.getName(), config.getLoggerClass(), "getLoggerClass");
    Assertions.assertEquals("debug", config.getLogLevel(), "getLogLevel");
    Assertions.assertFalse(config.shouldFilterMetrics(), "shouldFilterMetrics");
    Assertions.assertNull(config.getAllowedMetricsPath(), "getAllowedMetricsPath");
  }

  @Test
  public void testSettingEverything()
  {
    final Properties props = new Properties();
    props.setProperty("org.apache.druid.java.util.emitter.loggerClass", "Foo");
    props.setProperty("org.apache.druid.java.util.emitter.logLevel", "INFO");
    props.setProperty("org.apache.druid.java.util.emitter.shouldFilterMetrics", "true");
    props.setProperty("org.apache.druid.java.util.emitter.allowedMetricsPath", "/tmp/allowedMetrics.json");

    final ObjectMapper objectMapper = new ObjectMapper();
    final LoggingEmitterConfig config = objectMapper.convertValue(
        Emitters.makeCustomFactoryMap(props),
        LoggingEmitterConfig.class
    );

    Assertions.assertEquals("Foo", config.getLoggerClass(), "getLoggerClass");
    Assertions.assertEquals("INFO", config.getLogLevel(), "getLogLevel");
    Assertions.assertTrue(config.shouldFilterMetrics(), "shouldFilterMetrics");
    Assertions.assertEquals("/tmp/allowedMetrics.json", config.getAllowedMetricsPath(), "getAllowedMetricsPath");
  }

  @Test
  public void testSettingEverythingLegacy()
  {
    final Properties props = new Properties();
    props.setProperty("org.apache.druid.java.util.emitter.logging.class", "Foo");
    props.setProperty("org.apache.druid.java.util.emitter.logging.level", "INFO");
    props.setProperty("org.apache.druid.java.util.emitter.logging.shouldFilterMetrics", "true");
    props.setProperty("org.apache.druid.java.util.emitter.logging.allowedMetricsPath", "/custom/path.json");

    final ObjectMapper objectMapper = new ObjectMapper();
    final LoggingEmitterConfig config = objectMapper.convertValue(
        Emitters.makeLoggingMap(props),
        LoggingEmitterConfig.class
    );

    Assertions.assertEquals("Foo", config.getLoggerClass(), "getLoggerClass");
    Assertions.assertEquals("INFO", config.getLogLevel(), "getLogLevel");
    Assertions.assertTrue(config.shouldFilterMetrics(), "shouldFilterMetrics");
    Assertions.assertEquals("/custom/path.json", config.getAllowedMetricsPath(), "getAllowedMetricsPath");
  }
}
