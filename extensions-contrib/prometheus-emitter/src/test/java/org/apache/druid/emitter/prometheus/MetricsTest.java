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
import org.apache.druid.emitter.prometheus.metrics.Counter;
import org.apache.druid.emitter.prometheus.metrics.Gauge;
import org.apache.druid.emitter.prometheus.metrics.Histogram;
import org.apache.druid.emitter.prometheus.metrics.Timer;
import org.apache.druid.error.DruidException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class MetricsTest {
    private Metrics metrics;
    @Before
    public void setUp() {
       Map<String, String> extraLabels = new HashMap<>();
       extraLabels.put("extra_label", "value");

       CollectorRegistry.defaultRegistry.clear();
        PrometheusEmitterConfig prometheusEmitterConfig = new PrometheusEmitterConfig(
                null, "test", null, null, null,
                true, true, null, extraLabels, null, null
        );
        metrics = new Metrics(prometheusEmitterConfig);
    }
    @Test
    public void testTimerMetricInitialization() {
        Timer timer = (Timer) metrics.getByName("query/time", "historical");

        Assert.assertNotNull(timer);
        Assert.assertEquals("dataSource", timer.getDimensions()[0]);
        Assert.assertEquals("druid_service", timer.getDimensions()[1]);
        Assert.assertEquals("extra_label", timer.getDimensions()[2]);
        Assert.assertEquals("host_name", timer.getDimensions()[3]);
        Assert.assertEquals("type", timer.getDimensions()[4]);
        Assert.assertEquals(1000, timer.getConversionFactor(), 0.0);
    }
    @Test
    public void testGaugeMetricInitialization() {
        Gauge gauge = (Gauge) metrics.getByName("segment/loadQueue/count", "historical");

        Assert.assertNotNull(gauge);
        Assert.assertEquals("druid_service", gauge.getDimensions()[0]);
        Assert.assertEquals("extra_label", gauge.getDimensions()[1]);
        Assert.assertEquals("host_name", gauge.getDimensions()[2]);
        Assert.assertEquals("server", gauge.getDimensions()[3]);
    }
    @Test
    public void testHistogramMetricInitialization() {
        Histogram histogram = (Histogram) metrics.getByName("sqlQuery/time", null);

        Assert.assertNotNull(histogram);
        Assert.assertEquals("dataSource", histogram.getDimensions()[0]);
        Assert.assertEquals("druid_service", histogram.getDimensions()[1]);
        Assert.assertEquals("extra_label", histogram.getDimensions()[2]);
        Assert.assertEquals("host_name", histogram.getDimensions()[3]);
    }

    @Test
    public void testCounterMetricInitialization() {
        Counter counter = (Counter) metrics.getByName("query/count", null);

        Assert.assertNotNull(counter);
        Assert.assertEquals("druid_service", counter.getDimensions()[0]);
        Assert.assertEquals("extra_label", counter.getDimensions()[1]);
        Assert.assertEquals("host_name", counter.getDimensions()[2]);
    }

    @Test
    public void testMetricsConfigurationWithBadExtraLabels() {
        Map<String, String> extraLabels = new HashMap<>();
        extraLabels.put("extra label", "value");

        CollectorRegistry.defaultRegistry.clear();
        Exception exception = Assert.assertThrows(DruidException.class, () -> {
            new PrometheusEmitterConfig(
                null, "test", null, null, null,
                true, true, null, extraLabels, null, null
            );
        });

        String expectedMessage = "Invalid metric label name [extra label]";
        String actualMessage = exception.getMessage();

        Assert.assertTrue(actualMessage.contains(expectedMessage));
    }
}
