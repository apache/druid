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

import io.prometheus.client.Histogram;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class MetricsTest
{
  @Test
  public void testMetricsConfiguration()
  {
    Metrics metrics = new Metrics("test", null, true, true, null);
    DimensionsAndCollector dimensionsAndCollector = metrics.getByName("query/time", "historical");
    Assert.assertNotNull(dimensionsAndCollector);
    String[] dimensions = dimensionsAndCollector.getDimensions();
    Assert.assertEquals("dataSource", dimensions[0]);
    Assert.assertEquals("druid_service", dimensions[1]);
    Assert.assertEquals("host_name", dimensions[2]);
    Assert.assertEquals("type", dimensions[3]);
    Assert.assertEquals(1000.0, dimensionsAndCollector.getConversionFactor(), 0.0);
    Assert.assertTrue(dimensionsAndCollector.getCollector() instanceof Histogram);

    DimensionsAndCollector d = metrics.getByName("segment/loadQueue/count", "historical");
    Assert.assertNotNull(d);
    String[] dims = d.getDimensions();
    Assert.assertEquals("druid_service", dims[0]);
    Assert.assertEquals("host_name", dims[1]);
    Assert.assertEquals("server", dims[2]);
  }

  @Test
  public void testMetricsConfigurationWithExtraLabels()
  {
    Map<String, String> extraLabels = new HashMap<>();
    extraLabels.put("extra_label", "value");

    Metrics metrics = new Metrics("test_2", null, true, true, extraLabels);
    DimensionsAndCollector dimensionsAndCollector = metrics.getByName("query/time", "historical");
    Assert.assertNotNull(dimensionsAndCollector);
    String[] dimensions = dimensionsAndCollector.getDimensions();
    Assert.assertEquals("dataSource", dimensions[0]);
    Assert.assertEquals("druid_service", dimensions[1]);
    Assert.assertEquals("extra_label", dimensions[2]);
    Assert.assertEquals("host_name", dimensions[3]);
    Assert.assertEquals("type", dimensions[4]);
    Assert.assertEquals(1000.0, dimensionsAndCollector.getConversionFactor(), 0.0);
    Assert.assertTrue(dimensionsAndCollector.getCollector() instanceof Histogram);

    DimensionsAndCollector d = metrics.getByName("segment/loadQueue/count", "historical");
    Assert.assertNotNull(d);
    String[] dims = d.getDimensions();
    Assert.assertEquals("druid_service", dims[0]);
    Assert.assertEquals("extra_label", dims[1]);
    Assert.assertEquals("host_name", dims[2]);
    Assert.assertEquals("server", dims[3]);
  }
  
  @Test
  public void testMetricsConfigurationWithBadExtraLabels()
  {
    Map<String, String> extraLabels = new HashMap<>();
    extraLabels.put("extra label", "value");

    // Expect an exception thrown by Prometheus code due to invalid metric label
    Exception exception = Assert.assertThrows(IllegalArgumentException.class, () -> {
      new Metrics("test_3", null, true, true, extraLabels);
    });

    String expectedMessage = "Invalid metric label name: extra label";
    String actualMessage = exception.getMessage();

    Assert.assertTrue(actualMessage.contains(expectedMessage));
  }
}
