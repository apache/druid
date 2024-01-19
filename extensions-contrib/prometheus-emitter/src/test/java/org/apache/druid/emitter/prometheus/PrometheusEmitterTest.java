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
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;

public class PrometheusEmitterTest
{
  @Test
  public void testEmitterWithServiceLabel()
  {
    CollectorRegistry.defaultRegistry.clear();
    PrometheusEmitterConfig config = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.exporter, null, null, 0, null, false, true, 60, null, false, null);
    PrometheusEmitterModule prometheusEmitterModule = new PrometheusEmitterModule();
    Emitter emitter = prometheusEmitterModule.getEmitter(config);
    ServiceMetricEvent build = ServiceMetricEvent.builder()
                                                 .setDimension("server", "druid-data01.vpc.region")
                                                 .setMetric("segment/loadQueue/count", 10)
                                                 .build(ImmutableMap.of("service", "historical", "host", "druid.test.cn"));
    Assert.assertEquals("historical", build.getService());
    Assert.assertEquals("druid.test.cn", build.getHost());
    Assert.assertFalse(build.getUserDims().isEmpty());
    emitter.emit(build);
    Double count = CollectorRegistry.defaultRegistry.getSampleValue(
        "druid_segment_loadqueue_count", new String[]{"druid_service", "server"}, new String[]{"historical", "druid_data01_vpc_region"}
    );
    Assert.assertEquals(10, count.intValue());
  }

  @Test
  public void testEmitterWithServiceAndHostLabel()
  {
    CollectorRegistry.defaultRegistry.clear();
    PrometheusEmitterConfig config = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.exporter, null, null, 0, null, true, true, 60, null, false, null);
    PrometheusEmitterModule prometheusEmitterModule = new PrometheusEmitterModule();
    Emitter emitter = prometheusEmitterModule.getEmitter(config);
    ServiceMetricEvent build = ServiceMetricEvent.builder()
            .setDimension("server", "druid-data01.vpc.region")
            .setMetric("segment/loadQueue/count", 10)
            .build(ImmutableMap.of("service", "historical", "host", "druid.test.cn"));
    Assert.assertEquals("historical", build.getService());
    Assert.assertEquals("druid.test.cn", build.getHost());
    Assert.assertFalse(build.getUserDims().isEmpty());
    emitter.emit(build);
    Double count = CollectorRegistry.defaultRegistry.getSampleValue(
            "druid_segment_loadqueue_count", new String[]{"druid_service", "host_name", "server"}, new String[]{"historical", "druid.test.cn", "druid_data01_vpc_region"}
    );
    Assert.assertEquals(10, count.intValue());
  }

  @Test
  public void testEmitterWithExtraLabels()
  {
    CollectorRegistry.defaultRegistry.clear();
    Map<String, String> extraLabels = new HashMap<>();
    extraLabels.put("labelName", "labelValue");
    PrometheusEmitterConfig config = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.exporter, null, null, 0, null, false, false, 60, extraLabels, false, null);
    PrometheusEmitterModule prometheusEmitterModule = new PrometheusEmitterModule();
    Emitter emitter = prometheusEmitterModule.getEmitter(config);
    ServiceMetricEvent build = ServiceMetricEvent.builder()
                                                 .setDimension("server", "druid-data01.vpc.region")
                                                 .setMetric("segment/loadQueue/count", 10)
                                                 .build(ImmutableMap.of("service", "historical", "host", "druid.test.cn"));
    emitter.emit(build);
    Double count = CollectorRegistry.defaultRegistry.getSampleValue(
        "druid_segment_loadqueue_count",
        new String[]{"labelName", "server"},
        new String[]{"labelValue", "druid_data01_vpc_region"}
    );
    Assert.assertEquals(10, count.intValue());
  }

  @Test
  public void testEmitterWithServiceLabelAndExtraLabel()
  {
    CollectorRegistry.defaultRegistry.clear();
    Map<String, String> extraLabels = new HashMap<>();
    extraLabels.put("labelName", "labelValue");
    PrometheusEmitterConfig config = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.exporter, null, null, 0, null, false, true, 60, extraLabels, false, null);
    PrometheusEmitterModule prometheusEmitterModule = new PrometheusEmitterModule();
    Emitter emitter = prometheusEmitterModule.getEmitter(config);
    ServiceMetricEvent build = ServiceMetricEvent.builder()
                                                 .setDimension("server", "druid-data01.vpc.region")
                                                 .setMetric("segment/loadQueue/count", 10)
                                                 .build(ImmutableMap.of("service", "historical", "host", "druid.test.cn"));
    emitter.emit(build);
    Double count = CollectorRegistry.defaultRegistry.getSampleValue(
        "druid_segment_loadqueue_count",
        new String[]{"druid_service", "labelName", "server"},
        new String[]{"historical", "labelValue", "druid_data01_vpc_region"}
    );
    Assert.assertEquals(10, count.intValue());
  }

  @Test
  public void testEmitterWithEmptyExtraLabels()
  {
    CollectorRegistry.defaultRegistry.clear();
    Map<String, String> extraLabels = new HashMap<>();
    PrometheusEmitterConfig config = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.exporter, null, null, 0, null, false, true, 60, extraLabels, false, null);
    PrometheusEmitterModule prometheusEmitterModule = new PrometheusEmitterModule();
    Emitter emitter = prometheusEmitterModule.getEmitter(config);
    ServiceMetricEvent build = ServiceMetricEvent.builder()
                                                 .setDimension("server", "druid-data01.vpc.region")
                                                 .setMetric("segment/loadQueue/count", 10)
                                                 .build(ImmutableMap.of("service", "historical", "host", "druid.test.cn"));
    emitter.emit(build);
    Double count = CollectorRegistry.defaultRegistry.getSampleValue(
        "druid_segment_loadqueue_count",
        new String[]{"druid_service", "server"},
        new String[]{"historical", "druid_data01_vpc_region"}
    );
    Assert.assertEquals(10, count.intValue());
  }

  @Test
  public void testEmitterWithMultipleExtraLabels()
  {
    CollectorRegistry.defaultRegistry.clear();
    Map<String, String> extraLabels = new HashMap<>();
    extraLabels.put("labelName1", "labelValue1");
    extraLabels.put("labelName2", "labelValue2");
    PrometheusEmitterConfig config = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.exporter, null, null, 0, null, false, true, 60, extraLabels, false, null);
    PrometheusEmitterModule prometheusEmitterModule = new PrometheusEmitterModule();
    Emitter emitter = prometheusEmitterModule.getEmitter(config);
    ServiceMetricEvent build = ServiceMetricEvent.builder()
                                                 .setDimension("server", "druid-data01.vpc.region")
                                                 .setMetric("segment/loadQueue/count", 10)
                                                 .build(ImmutableMap.of("service", "historical", "host", "druid.test.cn"));
    emitter.emit(build);
    Double count = CollectorRegistry.defaultRegistry.getSampleValue(
        "druid_segment_loadqueue_count",
        new String[]{"druid_service", "labelName1", "labelName2", "server"},
        new String[]{"historical", "labelValue1", "labelValue2", "druid_data01_vpc_region"}
    );
    Assert.assertEquals(10, count.intValue());
  }

  @Test
  public void testEmitterWithLabelCollision()
  {
    CollectorRegistry.defaultRegistry.clear();
    // ExtraLabels contains a label that collides with a service label
    Map<String, String> extraLabels = new HashMap<>();
    extraLabels.put("server", "collisionLabelValue");
    PrometheusEmitterConfig config = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.exporter, null, null, 0, null, false, true, 60, extraLabels, false, null);
    PrometheusEmitterModule prometheusEmitterModule = new PrometheusEmitterModule();
    Emitter emitter = prometheusEmitterModule.getEmitter(config);
    ServiceMetricEvent build = ServiceMetricEvent.builder()
                                                 .setDimension("server", "druid-data01.vpc.region")
                                                 .setMetric("segment/loadQueue/count", 10)
                                                 .build(ImmutableMap.of("service", "historical", "host", "druid.test.cn"));
    emitter.emit(build);
    Double count = CollectorRegistry.defaultRegistry.getSampleValue(
        "druid_segment_loadqueue_count",
        new String[]{"druid_service", "server"},
        new String[]{"historical", "druid_data01_vpc_region"}
    );
    // Check that the extraLabel did not override the service label
    Assert.assertEquals(10, count.intValue());
    // Check that the extraLabel is not present in the CollectorRegistry
    Assert.assertNull(CollectorRegistry.defaultRegistry.getSampleValue(
        "druid_segment_loadqueue_count",
        new String[]{"druid_service", "server"},
        new String[]{"historical", "collisionLabelValue"}
    ));
  }

  @Test
  public void testEmitterMetric()
  {
    CollectorRegistry.defaultRegistry.clear();
    PrometheusEmitterConfig config = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.pushgateway, "namespace", null, 0, "pushgateway", true, true, 60, null, false, null);
    PrometheusEmitterModule prometheusEmitterModule = new PrometheusEmitterModule();
    Emitter emitter = prometheusEmitterModule.getEmitter(config);
    ServiceMetricEvent build = ServiceMetricEvent.builder()
            .setDimension("dataSource", "test")
            .setDimension("taskType", "index_parallel")
            .setMetric("task/run/time", 500)
            .build(ImmutableMap.of("service", "overlord", "host", "druid.test.cn"));
    emitter.emit(build);
    double assertEpsilon = 0.0001;
    Assert.assertEquals(0.0, CollectorRegistry.defaultRegistry.getSampleValue(
            "namespace_task_run_time_bucket", new String[]{"dataSource", "druid_service", "host_name", "taskType", "le"}, new String[]{"test", "overlord", "druid.test.cn", "index_parallel", "0.1"}
    ), assertEpsilon);
    Assert.assertEquals(1.0, CollectorRegistry.defaultRegistry.getSampleValue(
            "namespace_task_run_time_bucket", new String[]{"dataSource", "druid_service", "host_name", "taskType", "le"}, new String[]{"test", "overlord", "druid.test.cn", "index_parallel", "0.5"}
    ), assertEpsilon);
  }

  @Test
  public void testEmitterStart()
  {
    PrometheusEmitterConfig exportEmitterConfig = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.exporter, "namespace1", null, 0, null, true, true, 60, null, false, null);
    PrometheusEmitter exportEmitter = new PrometheusEmitter(exportEmitterConfig);
    exportEmitter.start();
    Assert.assertNotNull(exportEmitter.getServer());

    PrometheusEmitterConfig pushEmitterConfig = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.pushgateway, "namespace2", null, 0, "pushgateway", true, true, 60, null, false, null);
    PrometheusEmitter pushEmitter = new PrometheusEmitter(pushEmitterConfig);
    pushEmitter.start();
    Assert.assertNotNull(pushEmitter.getPushGateway());
  }

  @Test
  public void testEmitterPush() throws IOException
  {
    PrometheusEmitterConfig emitterConfig = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.pushgateway, "namespace3", null, 0, "pushgateway", true, true, 60, null, false, null);

    PushGateway mockPushGateway = mock(PushGateway.class);
    mockPushGateway.push(anyObject(Collector.class), anyString(), anyObject(ImmutableMap.class));

    PrometheusEmitter emitter = new PrometheusEmitter(emitterConfig);
    emitter.start();
    emitter.setPushGateway(mockPushGateway);
    ServiceMetricEvent build = ServiceMetricEvent.builder()
            .setDimension("task", "index_parallel")
            .setMetric("task/run/time", 500)
            .build(ImmutableMap.of("service", "peon", "host", "druid.test.cn"));
    emitter.emit(build);
    emitter.flush();
  }

  @Test
  public void testEmitterConfigCreationWithNullAsAddress()
  {
    // pushGatewayAddress can be null if it's exporter mode
    new PrometheusEmitterConfig(
        PrometheusEmitterConfig.Strategy.exporter,
        "namespace5",
        null,
        1,
        null,
        true,
        true,
        60, 
        null,
        false,
        null
    );

    Assert.assertThrows(
        "For `pushgateway` strategy, pushGatewayAddress must be specified.",
        IllegalArgumentException.class,
        () -> new PrometheusEmitterConfig(
            PrometheusEmitterConfig.Strategy.pushgateway,
            "namespace5",
            null,
            null,
            null,
            true,
            true,
            50,
            null,
            false,
            null
        )
    );
  }

  @Test
  public void testEmitterStartWithHttpUrl()
  {
    PrometheusEmitterConfig pushEmitterConfig = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.pushgateway, "namespace4", null, 0, "http://pushgateway", true, true, 60, null, false, null);
    PrometheusEmitter pushEmitter = new PrometheusEmitter(pushEmitterConfig);
    pushEmitter.start();
    Assert.assertNotNull(pushEmitter.getPushGateway());
  }

  @Test
  public void testEmitterStartWithHttpsUrl()
  {
    PrometheusEmitterConfig pushEmitterConfig = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.pushgateway, "namespace5", null, 0, "https://pushgateway", true, true, 60, null, false, null);
    PrometheusEmitter pushEmitter = new PrometheusEmitter(pushEmitterConfig);
    pushEmitter.start();
    Assert.assertNotNull(pushEmitter.getPushGateway());
  }

  @Test
  public void testEmitterConfig()
  {
    Assert.assertThrows(
        "For `exporter` strategy, port must be specified.",
        IllegalArgumentException.class,
        () -> new PrometheusEmitterConfig(
            PrometheusEmitterConfig.Strategy.exporter,
            "namespace5",
            null,
            null,
            "https://pushgateway",
            true,
            true,
            60,
            null,
            false,
            null
        )
    );

    // For pushgateway strategy, port can be null
    new PrometheusEmitterConfig(
        PrometheusEmitterConfig.Strategy.pushgateway,
        "namespace5",
        null,
        null,
        "https://pushgateway",
        true,
        true,
        60,
        null,
        false,
        null
    );
  }

  @Test
  public void testEmitterWithDeleteOnShutdown() throws IOException
  {
    PrometheusEmitterConfig emitterConfig = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.pushgateway, "namespace3", null, 0, "pushgateway", true, true, 60, null, true, null);

    PushGateway mockPushGateway = mock(PushGateway.class);
    mockPushGateway.push(anyObject(CollectorRegistry.class), anyString(), anyObject(ImmutableMap.class));
    expectLastCall().atLeastOnce();
    mockPushGateway.delete(anyString(), anyObject(ImmutableMap.class));
    expectLastCall().once();

    EasyMock.replay(mockPushGateway);

    PrometheusEmitter emitter = new PrometheusEmitter(emitterConfig);
    emitter.start();
    emitter.setPushGateway(mockPushGateway);
    ServiceMetricEvent build = ServiceMetricEvent.builder()
                                                 .setDimension("task", "index_parallel")
                                                 .setMetric("task/run/time", 500)
                                                 .build(ImmutableMap.of("service", "peon", "host", "druid.test.cn"));
    emitter.emit(build);
    emitter.flush();
    emitter.close();

    EasyMock.verify(mockPushGateway);
  }

  @Test
  public void testEmitterWithDeleteOnShutdownAndWait() throws IOException
  {
    PrometheusEmitterConfig emitterConfig = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.pushgateway, "namespace6", null, 0, "pushgateway", true, true, 60, null, true, 1_000L);

    PushGateway mockPushGateway = mock(PushGateway.class);
    mockPushGateway.push(anyObject(CollectorRegistry.class), anyString(), anyObject(ImmutableMap.class));
    expectLastCall().atLeastOnce();
    mockPushGateway.delete(anyString(), anyObject(ImmutableMap.class));
    expectLastCall().once();

    EasyMock.replay(mockPushGateway);

    PrometheusEmitter emitter = new PrometheusEmitter(emitterConfig);
    emitter.start();
    emitter.setPushGateway(mockPushGateway);
    ServiceMetricEvent build = ServiceMetricEvent.builder()
                                                 .setDimension("task", "index_parallel")
                                                 .setMetric("task/run/time", 500)
                                                 .build(ImmutableMap.of("service", "peon", "host", "druid.test.cn"));
    emitter.emit(build);
    emitter.flush();
    emitter.close();

    EasyMock.verify(mockPushGateway);
  }

  @Test
  public void testEmitterWithoutDeleteOnShutdown() throws IOException
  {
    PrometheusEmitterConfig emitterConfig = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.pushgateway, "namespace3", null, 0, "pushgateway", true, true, 60, null, false, null);

    PushGateway mockPushGateway = mock(PushGateway.class);
    mockPushGateway.push(anyObject(CollectorRegistry.class), anyString(), anyObject(ImmutableMap.class));
    expectLastCall().atLeastOnce();

    EasyMock.replay(mockPushGateway);

    PrometheusEmitter emitter = new PrometheusEmitter(emitterConfig);
    emitter.start();
    emitter.setPushGateway(mockPushGateway);
    ServiceMetricEvent build = ServiceMetricEvent.builder()
                                                 .setDimension("task", "index_parallel")
                                                 .setMetric("task/run/time", 500)
                                                 .build(ImmutableMap.of("service", "peon", "host", "druid.test.cn"));
    emitter.emit(build);
    emitter.flush();
    emitter.close();

    EasyMock.verify(mockPushGateway);
  }
}
