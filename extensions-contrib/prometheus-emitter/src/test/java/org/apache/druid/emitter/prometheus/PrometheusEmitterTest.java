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

import com.google.common.collect.ImmutableMap;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.mock;


public class PrometheusEmitterTest
{
  @Test
  public void testEmitter() 
  {
    PrometheusEmitterConfig config = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.exporter, null, null, 0, null);
    PrometheusEmitterModule prometheusEmitterModule = new PrometheusEmitterModule();
    Emitter emitter = prometheusEmitterModule.getEmitter(config);
    ServiceMetricEvent build = ServiceMetricEvent.builder()
                                                 .setDimension("server", "druid-data01.vpc.region")
                                                 .build("segment/loadQueue/count", 10)
                                                 .build(ImmutableMap.of("service", "historical"));
    Assert.assertEquals("historical", build.getService());
    Assert.assertFalse(build.getUserDims().isEmpty());
    emitter.emit(build);
    Double count = CollectorRegistry.defaultRegistry.getSampleValue(
        "druid_segment_loadqueue_count", new String[]{"server"}, new String[]{"druid_data01_vpc_region"}
    );
    Assert.assertEquals(10, count.intValue());
  }

  @Test
  public void testEmitterMetric()
  {
    PrometheusEmitterConfig config = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.pushgateway, "namespace", null, 0, "pushgateway");
    PrometheusEmitterModule prometheusEmitterModule = new PrometheusEmitterModule();
    Emitter emitter = prometheusEmitterModule.getEmitter(config);
    ServiceMetricEvent build = ServiceMetricEvent.builder()
            .setDimension("dataSource", "test")
            .setDimension("taskType", "index_parallel")
            .build("task/run/time", 500)
            .build(ImmutableMap.of("service", "overlord"));
    emitter.emit(build);
    double assertEpsilon = 0.0001;
    Assert.assertEquals(0.0, CollectorRegistry.defaultRegistry.getSampleValue(
            "namespace_task_run_time_bucket", new String[]{"dataSource", "taskType", "le"}, new String[]{"test", "index_parallel", "0.1"}
    ), assertEpsilon);
    Assert.assertEquals(1.0, CollectorRegistry.defaultRegistry.getSampleValue(
            "namespace_task_run_time_bucket", new String[]{"dataSource", "taskType", "le"}, new String[]{"test", "index_parallel", "0.5"}
    ), assertEpsilon);
  }

  @Test
  public void testEmitterStart()
  {
    PrometheusEmitterConfig exportEmitterConfig = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.exporter, "namespace1", null, 0, null);
    PrometheusEmitter exportEmitter = new PrometheusEmitter(exportEmitterConfig);
    exportEmitter.start();
    Assert.assertNotNull(exportEmitter.getServer());

    PrometheusEmitterConfig pushEmitterConfig = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.pushgateway, "namespace2", null, 0, "pushgateway");
    PrometheusEmitter pushEmitter = new PrometheusEmitter(pushEmitterConfig);
    pushEmitter.start();
    Assert.assertNotNull(pushEmitter.getPushGateway());
  }

  @Test
  public void testEmitterPush() throws IOException
  {
    PrometheusEmitterConfig emitterConfig = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.pushgateway, "namespace3", null, 0, "pushgateway");

    PushGateway mockPushGateway = mock(PushGateway.class);
    mockPushGateway.push(anyObject(Collector.class), anyString(), anyObject(ImmutableMap.class));

    PrometheusEmitter emitter = new PrometheusEmitter(emitterConfig);
    emitter.start();
    emitter.setPushGateway(mockPushGateway);
    ServiceMetricEvent build = ServiceMetricEvent.builder()
            .setDimension("task", "index_parallel")
            .build("task/run/time", 500)
            .build(ImmutableMap.of("service", "peon"));
    emitter.emit(build);
    emitter.flush();
  }
}
