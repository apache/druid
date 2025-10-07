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

package org.apache.druid.emitter.prometheus;

import io.prometheus.client.CollectorRegistry;
import org.apache.druid.error.DruidException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PrometheusEmitterConfigTest
{
  @Test
  public void testEmitterConfigWithBadExtraLabels()
  {
    CollectorRegistry.defaultRegistry.clear();

    Map<String, String> extraLabels = new HashMap<>();
    extraLabels.put("label Name", "label Value");

    // Expect an exception thrown by our own PrometheusEmitterConfig due to invalid label key
    Exception exception = Assert.assertThrows(DruidException.class, () -> {
      new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.exporter, null, null, 0, null, false, true, 60, extraLabels, false, null);
    });

    String expectedMessage = "Invalid metric label name [label Name]. Label names must conform to the pattern [[a-zA-Z_:][a-zA-Z0-9_:]*]";
    String actualMessage = exception.getMessage();

    Assert.assertTrue(actualMessage.contains(expectedMessage));
  }

  @Test
  public void testDefaultConstructor()
  {
    PrometheusEmitterConfig config = new PrometheusEmitterConfig(null, null, null, null, null, false, false, null, null, null, null);
    Assert.assertEquals(PrometheusEmitterConfig.Strategy.exporter, config.getStrategy());
    Assert.assertEquals("druid", config.getNamespace());
    Assert.assertNull(config.getDimensionMapPath());
  }

  @Test
  public void testExporterStrategy()
  {
    PrometheusEmitterConfig config = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.exporter, "druid", null, 8080, null, true, true, null, null, null, null);
    Assert.assertEquals(PrometheusEmitterConfig.Strategy.exporter, config.getStrategy());
    Assert.assertEquals("druid", config.getNamespace());
    Assert.assertEquals(8080, config.getPort());
    Assert.assertTrue(config.isAddHostAsLabel());
    Assert.assertTrue(config.isAddHostAsLabel());

    Assert.assertThrows(IllegalArgumentException.class, () -> {
      new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.exporter, null, null, null, null, false, false, null, null, null, null);
    });
  }

  @Test
  public void testPushgatewayStrategy()
  {
    PrometheusEmitterConfig config = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.pushgateway, "druid", null, null, "localhost:9091", false, false, 30, null, true, 5000L);
    Assert.assertEquals(PrometheusEmitterConfig.Strategy.pushgateway, config.getStrategy());
    Assert.assertEquals("druid", config.getNamespace());
    Assert.assertEquals("localhost:9091", config.getPushGatewayAddress());
    Assert.assertEquals(Integer.valueOf(30), config.getFlushPeriod());
    Assert.assertFalse(config.isAddHostAsLabel());

    Assert.assertThrows(IllegalArgumentException.class, () -> {
      new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.pushgateway, null, null, null, null, false, false, null, null, null, null);
    });
  }

  @Test
  public void testInvalidFlushPeriod()
  {
    IllegalArgumentException illegalArgumentException = Assert.assertThrows(IllegalArgumentException.class, () -> {
      new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.pushgateway, null, null, null, "localhost:9091", false, false, 0, null, null, null);
    });
    Assert.assertEquals("flushPeriod must be greater than 0.", illegalArgumentException.getMessage());

    illegalArgumentException = Assert.assertThrows(IllegalArgumentException.class, () -> {
      new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.exporter, null, null, 0, "localhost:9091", false, false, 0, null, null, null);
    });
    Assert.assertEquals("flushPeriod must be greater than 0.", illegalArgumentException.getMessage());
  }

  @Test
  public void testInvalidExtraLabelName()
  {
    DruidException druidException = Assert.assertThrows(DruidException.class, () -> {
      Map<String, String> extraLabels = new HashMap<>();
      extraLabels.put("invalid label", "value");
      new PrometheusEmitterConfig(null, null, null, null, null, false, false, null, extraLabels, null, null);
    });
    Assert.assertEquals("Invalid metric label name [invalid label]. Label names must conform to the pattern [[a-zA-Z_:][a-zA-Z0-9_:]*].", druidException.getMessage());
  }

  @Test
  public void testNegativeWaitForShutdownDelay()
  {
    DruidException druidException = Assert.assertThrows(DruidException.class, () -> {
      new PrometheusEmitterConfig(null, null, null, null, null, false, false, null, null, null, -1L);
    });
    Assert.assertEquals("Invalid value for waitForShutdownDelay[-1] specified, waitForShutdownDelay must be >= 0.", druidException.getMessage());
  }
}
