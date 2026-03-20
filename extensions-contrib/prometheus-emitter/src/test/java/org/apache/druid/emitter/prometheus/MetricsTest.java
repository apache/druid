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
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.ISE;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class MetricsTest
{
  @Test
  public void testMetricsConfiguration()
  {
    PrometheusEmitterConfig config = new PrometheusEmitterConfig(null, "test", null, null, null, true, true, null, null, null, null);
    Metrics metrics = new Metrics(config);
    DimensionsAndCollector dimensionsAndCollector = metrics.getByName("query/time", "historical");
    Assert.assertNotNull(dimensionsAndCollector);
    String[] dimensions = dimensionsAndCollector.getDimensions();
    Assert.assertEquals("dataSource", dimensions[0]);
    Assert.assertEquals("druid_service", dimensions[1]);
    Assert.assertEquals("host_name", dimensions[2]);
    Assert.assertEquals("type", dimensions[3]);
    Assert.assertEquals(1000.0, dimensionsAndCollector.getConversionFactor(), 0.0);
    double[] defaultHistogramBuckets = {0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0, 30.0, 60.0, 120.0, 300.0};
    Assert.assertArrayEquals(defaultHistogramBuckets, dimensionsAndCollector.getHistogramBuckets(), 0.0);
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

    PrometheusEmitterConfig config = new PrometheusEmitterConfig(null, "test_2", null, null, null, true, true, null, extraLabels, null, null);
    Metrics metrics = new Metrics(config);
    DimensionsAndCollector dimensionsAndCollector = metrics.getByName("query/time", "historical");
    Assert.assertNotNull(dimensionsAndCollector);
    String[] dimensions = dimensionsAndCollector.getDimensions();
    Assert.assertEquals("dataSource", dimensions[0]);
    Assert.assertEquals("druid_service", dimensions[1]);
    Assert.assertEquals("extra_label", dimensions[2]);
    Assert.assertEquals("host_name", dimensions[3]);
    Assert.assertEquals("type", dimensions[4]);
    Assert.assertEquals(1000.0, dimensionsAndCollector.getConversionFactor(), 0.0);
    double[] defaultHistogramBuckets = {0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0, 30.0, 60.0, 120.0, 300.0};
    Assert.assertArrayEquals(defaultHistogramBuckets, dimensionsAndCollector.getHistogramBuckets(), 0.0);
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
    Exception exception = Assert.assertThrows(DruidException.class, () -> {
      new Metrics(new PrometheusEmitterConfig(null, "test_3", null, null, null, true, true, null, extraLabels, null, null));
    });

    String expectedMessage = "Invalid metric label name [extra label]. Label names must conform to the pattern [[a-zA-Z_:][a-zA-Z0-9_:]*].";
    String actualMessage = exception.getMessage();

    Assert.assertTrue(actualMessage.contains(expectedMessage));
  }

  @Test
  public void testMetricsConfigurationWithNonExistentMetric()
  {
    PrometheusEmitterConfig config = new PrometheusEmitterConfig(null, "test_4", null, null, null, true, true, null, null, null, null);
    Metrics metrics = new Metrics(config);
    DimensionsAndCollector nonExistentDimsCollector = metrics.getByName("non/existent", "historical");
    Assert.assertNull(nonExistentDimsCollector);
  }

  @Test
  public void testMetricsConfigurationWithUnSupportedType()
  {
    PrometheusEmitterConfig config = new PrometheusEmitterConfig(null, "test_5", "src/test/resources/defaultInvalidMetricsTest.json", null, null, true, true, null, null, null, null);
    ISE iseException = Assert.assertThrows(ISE.class, () -> {
      new Metrics(config);
    });
    Assert.assertEquals("Failed to parse metric configuration", iseException.getMessage());
  }

  @Test
  public void testMetricsConfigurationWithTimerHistogramBuckets()
  {
    PrometheusEmitterConfig config = new PrometheusEmitterConfig(null, "test_6", "src/test/resources/defaultMetricsTest.json", null, null, true, true, null, null, null, null);
    Metrics metrics = new Metrics(config);
    DimensionsAndCollector dimensionsAndCollector = metrics.getByName("query/time", "historical");
    Assert.assertNotNull(dimensionsAndCollector);
    String[] dimensions = dimensionsAndCollector.getDimensions();
    Assert.assertEquals("dataSource", dimensions[0]);
    Assert.assertEquals("druid_service", dimensions[1]);
    Assert.assertEquals("host_name", dimensions[2]);
    Assert.assertEquals("type", dimensions[3]);
    Assert.assertEquals(1000.0, dimensionsAndCollector.getConversionFactor(), 0.0);
    double[] expectedHistogramBuckets = {10.0, 30.0, 60.0, 120.0, 200.0, 300.0};
    Assert.assertArrayEquals(expectedHistogramBuckets, dimensionsAndCollector.getHistogramBuckets(), 0.0);
  }

}
