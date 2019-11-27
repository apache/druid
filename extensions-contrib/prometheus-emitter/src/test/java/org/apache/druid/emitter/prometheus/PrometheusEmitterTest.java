package org.apache.druid.emitter.prometheus;

import com.google.common.collect.ImmutableMap;
import io.prometheus.client.CollectorRegistry;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class PrometheusEmitterTest
{
  @Test
  public void testEmitter() {
    PrometheusEmitterConfig config = new PrometheusEmitterConfig(null, null, 0);
    PrometheusEmitter emitter = new PrometheusEmitter(config);
    ServiceMetricEvent build = ServiceMetricEvent.builder()
                                                 .setDimension("server", "druid-data01.vpc.region")
                                                 .build("segment/loadQueue/count", 10)
                                                 .build(ImmutableMap.of("service", "historical"));
    assertEquals("historical", build.getService());
    assertFalse(build.getUserDims().isEmpty());
    emitter.emit(build);
    Double count = CollectorRegistry.defaultRegistry.getSampleValue(
        "druid_segment_loadqueue_count", new String[]{"server"}, new String[]{"druid_data01_vpc_region"}
    );
    assertEquals(10, count.intValue());
  }
}
