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
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.exporter.PushGateway;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;

public class PrometheusEmitterTest
{
  @Test
  public void testEmitterWithServiceLabel()
  {
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
    Assert.assertTrue(exportEmitter.getServer() instanceof HTTPServer);
    exportEmitter.close();

    PrometheusEmitterConfig pushEmitterConfig = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.pushgateway, "namespace2", null, 0, "pushgateway", true, true, 60, null, false, null);
    PrometheusEmitter pushEmitter = new PrometheusEmitter(pushEmitterConfig);
    pushEmitter.start();
    Assert.assertNotNull(pushEmitter.getPushGateway());
    Assert.assertTrue(pushEmitter.getPushGateway() instanceof PushGateway);
    pushEmitter.close();
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

  @Test
  public void testMetricTtlExpiration() throws ExecutionException, InterruptedException
  {
    int flushPeriod = 3;
    PrometheusEmitterConfig config = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.exporter, "test", null, 0, null, true, true, flushPeriod, null, false, null);
    ScheduledExecutorService exec = ScheduledExecutors.fixed(1, "PrometheusTTLExecutor-%s");
    PrometheusEmitter emitter = new PrometheusEmitter(config, exec);
    emitter.start();

    ServiceMetricEvent event = ServiceMetricEvent.builder()
                                                 .setMetric("segment/loadQueue/count", 10)
                                                 .setDimension("server", "historical1")
                                                 .build(ImmutableMap.of("service", "historical", "host", "druid.test.cn"));
    emitter.emit(event);

    // Get the metrics and check that it's not expired initially
    Map<String, DimensionsAndCollector> registeredMetrics = emitter.getMetrics().getRegisteredMetrics();
    DimensionsAndCollector testMetric = registeredMetrics.get("segment/loadQueue/count");

    Assert.assertNotNull("Test metric should be registered", testMetric);
    Assert.assertFalse(
        "Metric should not be expired initially",
        testMetric.shouldRemoveIfExpired(Arrays.asList("historical", "druid.test.cn", "historical1"))
    );
    Assert.assertEquals(1, testMetric.getCollector().collect().get(0).samples.size());

    // Wait for the metric to expire (ttl + 1 second buffer)
    Thread.sleep(TimeUnit.SECONDS.toMillis(flushPeriod) + 1000);
    exec.submit(emitter::cleanUpStaleMetrics).get();
    Assert.assertEquals(0, testMetric.getCollector().collect().get(0).samples.size());
    emitter.close();
  }

  @Test
  public void testMetricTtlUpdate() throws ExecutionException, InterruptedException
  {
    int flushPeriod = 3;
    PrometheusEmitterConfig config = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.exporter, "test", null, 0, null, true, true, flushPeriod, null, false, null);
    ScheduledExecutorService exec = ScheduledExecutors.fixed(1, "PrometheusTTLExecutor-%s");
    PrometheusEmitter emitter = new PrometheusEmitter(config, exec);
    emitter.start();

    ServiceMetricEvent event = ServiceMetricEvent.builder()
                                                 .setMetric("segment/loadQueue/count", 10)
                                                 .setDimension("server", "historical1")
                                                 .build(ImmutableMap.of("service", "historical", "host", "druid.test.cn"));
    emitter.emit(event);

    // Get the metrics and check that it's not expired initially
    Map<String, DimensionsAndCollector> registeredMetrics = emitter.getMetrics().getRegisteredMetrics();
    DimensionsAndCollector testMetric = registeredMetrics.get("segment/loadQueue/count");

    Assert.assertNotNull(
        "Test metric should be registered",
        testMetric
    );
    Assert.assertFalse(
        "Metric should not be expired initially",
        testMetric.shouldRemoveIfExpired(Arrays.asList("historical", "druid.test.cn", "historical1"))
    );
    Assert.assertEquals(1, testMetric.getCollector().collect().get(0).samples.size());

    // Wait for a little, but not long enough for the metric to expire
    long waitTime = TimeUnit.SECONDS.toMillis(flushPeriod) / 5;
    Thread.sleep(waitTime);

    Assert.assertFalse(
        "Metric should not be expired",
        testMetric.shouldRemoveIfExpired(Arrays.asList("historical", "druid.test.cn", "historical1"))
    );
    exec.submit(emitter::cleanUpStaleMetrics).get();
    Assert.assertEquals(1, testMetric.getCollector().collect().get(0).samples.size());
    emitter.close();
  }

  @Test
  public void testMetricTtlUpdateWithDifferentLabels() throws ExecutionException, InterruptedException
  {
    int flushPeriod = 3;
    PrometheusEmitterConfig config = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.exporter, "test", null, 0, null, true, true, flushPeriod, null, false, null);
    ScheduledExecutorService exec = ScheduledExecutors.fixed(1, "PrometheusTTLExecutor-%s");
    PrometheusEmitter emitter = new PrometheusEmitter(config, exec);
    emitter.start();

    ServiceMetricEvent event1 = ServiceMetricEvent.builder()
                                                 .setMetric("segment/loadQueue/count", 10)
                                                 .setDimension("server", "historical1")
                                                 .build(ImmutableMap.of("service", "historical", "host", "druid.test.cn"));
    ServiceMetricEvent event2 = ServiceMetricEvent.builder()
                                                  .setMetric("segment/loadQueue/count", 10)
                                                  .setDimension("server", "historical2")
                                                  .build(ImmutableMap.of("service", "historical", "host", "druid.test.cn"));
    emitter.emit(event1);
    emitter.emit(event2);

    // Get the metrics and check that it's not expired initially
    Map<String, DimensionsAndCollector> registeredMetrics = emitter.getMetrics().getRegisteredMetrics();
    DimensionsAndCollector testMetric = registeredMetrics.get("segment/loadQueue/count");

    Assert.assertNotNull(
        "Test metric should be registered",
        testMetric
    );
    Assert.assertFalse(
        "Metric should not be expired initially",
        testMetric.shouldRemoveIfExpired(Arrays.asList("historical", "druid.test.cn", "historical1"))
    );
    Assert.assertFalse(
        "Metric should not be expired initially",
        testMetric.shouldRemoveIfExpired(Arrays.asList("historical", "druid.test.cn", "historical2"))
    );
    exec.submit(emitter::cleanUpStaleMetrics).get();
    Assert.assertEquals(2, testMetric.getCollector().collect().get(0).samples.size());

    // Wait for a little, but not long enough for the metric to expire
    long waitTime = TimeUnit.SECONDS.toMillis(flushPeriod) / 5;
    Thread.sleep(waitTime);

    Assert.assertFalse(
        "Metric should not be expired",
        testMetric.shouldRemoveIfExpired(Arrays.asList("historical", "druid.test.cn", "historical1"))
    );
    Assert.assertFalse(
        "Metric should not be expired",
        testMetric.shouldRemoveIfExpired(Arrays.asList("historical", "druid.test.cn", "historical2"))
    );
    exec.submit(emitter::cleanUpStaleMetrics).get();
    Assert.assertEquals(2, testMetric.getCollector().collect().get(0).samples.size());
    // Reset update time only for event2
    emitter.emit(event2);

    // Wait for the remainder of the TTL to allow event1 to expire
    Thread.sleep(waitTime * 4);

    exec.submit(emitter::cleanUpStaleMetrics).get();
    Assert.assertEquals(1, testMetric.getCollector().collect().get(0).samples.size());
    emitter.close();
  }

  @Test
  public void testLabelsNotTrackedWithTtlUnset()
  {
    PrometheusEmitterConfig flushPeriodNull = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.exporter, "test", null, 0, null, true, true, null, null, false, null);
    PrometheusEmitterConfig flushPeriodSet = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.exporter, "test", null, 0, null, true, true, 3, null, false, null);

    PrometheusEmitter emitter = new PrometheusEmitter(flushPeriodNull);
    ServiceMetricEvent event = ServiceMetricEvent.builder()
            .setMetric("segment/loadQueue/count", 10)
            .setDimension("server", "historical1")
            .build(ImmutableMap.of("service", "historical", "host", "druid.test.cn"));
    emitter.emit(event);
    DimensionsAndCollector metric = emitter.getMetrics().getRegisteredMetrics().get("segment/loadQueue/count");
    Assert.assertEquals(0, metric.getLabelValuesToStopwatch().size());
    emitter.close();
    CollectorRegistry.defaultRegistry.clear();

    emitter = new PrometheusEmitter(flushPeriodSet);
    emitter.emit(event);
    metric = emitter.getMetrics().getRegisteredMetrics().get("segment/loadQueue/count");
    Assert.assertEquals(1, metric.getLabelValuesToStopwatch().size());
    emitter.close();
  }

  @Test
  public void testCounterWithNegativeValue()
  {
    PrometheusEmitterConfig config = new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.exporter, "test", null, 0, null, true, true, null, null, false, null);
    PrometheusEmitter emitter = new PrometheusEmitter(config);
    ServiceMetricEvent event = ServiceMetricEvent.builder()
            .setMetric("segment/moveSkipped/count", -1)
            .setDimension("server", "historical1")
            .build(ImmutableMap.of("service", "historical", "host", "druid.test.cn"));
    emitter.emit(event);
    emitter.close();
  }

  @After
  public void tearDown()
  {
    CollectorRegistry.defaultRegistry.clear();
  }
}
