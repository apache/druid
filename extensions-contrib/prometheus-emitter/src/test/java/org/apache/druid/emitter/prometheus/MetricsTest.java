package org.apache.druid.emitter.prometheus;

import io.prometheus.client.CollectorRegistry;
import org.apache.druid.emitter.prometheus.metrics.Counter;
import org.apache.druid.emitter.prometheus.metrics.Gauge;
import org.apache.druid.emitter.prometheus.metrics.Histogram;
import org.apache.druid.emitter.prometheus.metrics.Timer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MetricsTest {
    private Metrics metrics;
    @Before
    public void setUp() {
       CollectorRegistry.defaultRegistry.clear();
        PrometheusEmitterConfig prometheusEmitterConfig = new PrometheusEmitterConfig(
                null, "test", null, null, null,
                true, true, null, null, null, null
        );
        metrics = new Metrics(prometheusEmitterConfig);
    }
    @Test
    public void testTimerMetricInitialization() {
        Timer timer = (Timer) metrics.getByName("query/time", "historical");

        Assert.assertNotNull(timer);
        Assert.assertEquals("dataSource", timer.getDimensions()[0]);
        Assert.assertEquals("druid_service", timer.getDimensions()[1]);
        Assert.assertEquals("host_name", timer.getDimensions()[2]);
        Assert.assertEquals("type", timer.getDimensions()[3]);
        Assert.assertEquals(1000, timer.getConversionFactor(), 0.0);
    }
    @Test
    public void testGaugeMetricInitialization() {
        Gauge gauge = (Gauge) metrics.getByName("segment/loadQueue/count", "historical");

        Assert.assertNotNull(gauge);
        Assert.assertEquals("druid_service", gauge.getDimensions()[0]);
        Assert.assertEquals("host_name", gauge.getDimensions()[1]);
        Assert.assertEquals("server", gauge.getDimensions()[2]);
    }
    @Test
    public void testHistogramMetricInitialization() {
        Histogram histogram = (Histogram) metrics.getByName("sqlQuery/time", null);

        Assert.assertNotNull(histogram);
        Assert.assertEquals("dataSource", histogram.getDimensions()[0]);
        Assert.assertEquals("druid_service", histogram.getDimensions()[1]);
        Assert.assertEquals("host_name", histogram.getDimensions()[2]);
    }

    @Test
    public void testCounterMetricInitialization() {
        Counter counter = (Counter) metrics.getByName("query/count", null);

        Assert.assertNotNull(counter);
        Assert.assertEquals("druid_service", counter.getDimensions()[0]);
        Assert.assertEquals("host_name", counter.getDimensions()[1]);
    }
}
