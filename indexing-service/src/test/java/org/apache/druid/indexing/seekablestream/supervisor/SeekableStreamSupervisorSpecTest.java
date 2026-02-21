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

package org.apache.druid.indexing.seekablestream.supervisor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.SupervisorTaskAutoScaler;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskClientFactory;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.AutoScalerConfig;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.LagBasedAutoScaler;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.LagBasedAutoScalerConfig;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.NoopTaskAutoScaler;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.metrics.DruidMonitorSchedulerConfig;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.metadata.TestSupervisorSpec;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.indexing.DataSchema;
import org.easymock.EasyMock;
import org.hamcrest.MatcherAssert;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.junit.Assert.assertThrows;

public class SeekableStreamSupervisorSpecTest extends SeekableStreamSupervisorTestBase
{
  private SeekableStreamSupervisorIngestionSpec ingestionSchema;
  private DataSchema dataSchema;
  private SeekableStreamSupervisorTuningConfig seekableStreamSupervisorTuningConfig;
  private SeekableStreamSupervisorIOConfig seekableStreamSupervisorIOConfig;
  private SupervisorStateManagerConfig supervisorConfig;
  private SeekableStreamSupervisor supervisor4;

  private SeekableStreamIndexTaskClientFactory indexTaskClientFactory;
  private ObjectMapper mapper;
  private DruidMonitorSchedulerConfig monitorSchedulerConfig;
  private SupervisorStateManagerConfig supervisorStateManagerConfig;

  @Before
  public void setUp()
  {
    ingestionSchema = EasyMock.mock(SeekableStreamSupervisorIngestionSpec.class);
    dataSchema = EasyMock.mock(DataSchema.class);
    seekableStreamSupervisorTuningConfig = EasyMock.mock(SeekableStreamSupervisorTuningConfig.class);
    seekableStreamSupervisorIOConfig = EasyMock.mock(SeekableStreamSupervisorIOConfig.class);
    supervisorConfig = new SupervisorStateManagerConfig();
    indexTaskClientFactory = EasyMock.mock(SeekableStreamIndexTaskClientFactory.class);
    mapper = new DefaultObjectMapper();
    monitorSchedulerConfig = EasyMock.mock(DruidMonitorSchedulerConfig.class);
    supervisorStateManagerConfig = EasyMock.mock(SupervisorStateManagerConfig.class);
    supervisor4 = EasyMock.mock(SeekableStreamSupervisor.class);

    EasyMock.expect(spec.getContextValue(DruidMetrics.TAGS)).andReturn(null).anyTimes();
  }

  @Test
  public void testAutoScalerConfig()
  {
    AutoScalerConfig autoScalerConfigEmpty = mapper.convertValue(new HashMap<>(), AutoScalerConfig.class);
    Assert.assertTrue(autoScalerConfigEmpty instanceof LagBasedAutoScalerConfig);
    Assert.assertFalse(autoScalerConfigEmpty.getEnableTaskAutoScaler());

    AutoScalerConfig autoScalerConfigNull = mapper.convertValue(null, AutoScalerConfig.class);
    Assert.assertNull(autoScalerConfigNull);

    AutoScalerConfig autoScalerConfigDefault = mapper.convertValue(
        ImmutableMap.of("autoScalerStrategy", "lagBased"),
        AutoScalerConfig.class
    );
    Assert.assertTrue(autoScalerConfigDefault instanceof LagBasedAutoScalerConfig);

    AutoScalerConfig autoScalerConfigValue = mapper.convertValue(
        ImmutableMap.of("lagCollectionIntervalMillis", "1"),
        AutoScalerConfig.class
    );
    Assert.assertTrue(autoScalerConfigValue instanceof LagBasedAutoScalerConfig);
    LagBasedAutoScalerConfig lagBasedAutoScalerConfig = (LagBasedAutoScalerConfig) autoScalerConfigValue;
    Assert.assertEquals(lagBasedAutoScalerConfig.getLagCollectionIntervalMillis(), 1);

    Exception e = null;
    try {
      AutoScalerConfig autoScalerError = mapper.convertValue(
          ImmutableMap.of(
              "enableTaskAutoScaler",
              "true",
              "taskCountMax",
              "1",
              "taskCountMin",
              "4"
          ), AutoScalerConfig.class
      );
    }
    catch (RuntimeException ex) {
      e = ex;
    }
    Assert.assertNotNull(e);

    e = null;
    try {
      // taskCountMax and taskCountMin couldn't be ignored.
      AutoScalerConfig autoScalerError2 = mapper.convertValue(
          ImmutableMap.of("enableTaskAutoScaler", "true"),
          AutoScalerConfig.class
      );
    }
    catch (RuntimeException ex) {
      e = ex;
    }
    Assert.assertNotNull(e);
  }

  @Test
  public void testAutoScalerCreated()
  {
    HashMap<String, Object> autoScalerConfig = new HashMap<>();
    autoScalerConfig.put("enableTaskAutoScaler", true);
    autoScalerConfig.put("lagCollectionIntervalMillis", 500);
    autoScalerConfig.put("lagCollectionRangeMillis", 500);
    autoScalerConfig.put("scaleOutThreshold", 5000000);
    autoScalerConfig.put("triggerScaleOutFractionThreshold", 0.3);
    autoScalerConfig.put("scaleInThreshold", 1000000);
    autoScalerConfig.put("triggerScaleInFractionThreshold", 0.8);
    autoScalerConfig.put("scaleActionStartDelayMillis", 0);
    autoScalerConfig.put("scaleActionPeriodMillis", 100);
    autoScalerConfig.put("taskCountMax", 8);
    autoScalerConfig.put("taskCountMin", 1);
    autoScalerConfig.put("scaleInStep", 1);
    autoScalerConfig.put("scaleOutStep", 2);
    autoScalerConfig.put("minTriggerScaleActionFrequencyMillis", 1200000);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(seekableStreamSupervisorIOConfig.getAutoScalerConfig())
            .andReturn(mapper.convertValue(autoScalerConfig, AutoScalerConfig.class))
            .anyTimes();
    EasyMock.expect(seekableStreamSupervisorIOConfig.getStream()).andReturn("stream").anyTimes();
    EasyMock.replay(seekableStreamSupervisorIOConfig);

    EasyMock.expect(supervisor4.getActiveTaskGroupsCount()).andReturn(0).anyTimes();
    EasyMock.expect(supervisor4.getIoConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.replay(supervisor4);

    TestSeekableStreamSupervisorSpec spec = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        null,
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        "id1"
    );
    SupervisorTaskAutoScaler autoscaler = spec.createAutoscaler(supervisor4);
    Assert.assertTrue(autoscaler instanceof LagBasedAutoScaler);

    EasyMock.reset(seekableStreamSupervisorIOConfig);
    autoScalerConfig.put("enableTaskAutoScaler", false);
    EasyMock.expect(seekableStreamSupervisorIOConfig.getAutoScalerConfig())
            .andReturn(mapper.convertValue(autoScalerConfig, AutoScalerConfig.class))
            .anyTimes();
    EasyMock.replay(seekableStreamSupervisorIOConfig);
    SupervisorTaskAutoScaler autoscaler2 = spec.createAutoscaler(supervisor4);
    Assert.assertTrue(autoscaler2 instanceof NoopTaskAutoScaler);

    EasyMock.reset(seekableStreamSupervisorIOConfig);
    autoScalerConfig.remove("enableTaskAutoScaler");
    EasyMock.expect(seekableStreamSupervisorIOConfig.getAutoScalerConfig())
            .andReturn(mapper.convertValue(autoScalerConfig, AutoScalerConfig.class))
            .anyTimes();
    EasyMock.replay(seekableStreamSupervisorIOConfig);
    SupervisorTaskAutoScaler autoscaler3 = spec.createAutoscaler(supervisor4);
    Assert.assertTrue(autoscaler3 instanceof NoopTaskAutoScaler);

    EasyMock.reset(seekableStreamSupervisorIOConfig);
    autoScalerConfig.clear();
    EasyMock.expect(seekableStreamSupervisorIOConfig.getAutoScalerConfig())
            .andReturn(mapper.convertValue(autoScalerConfig, AutoScalerConfig.class))
            .anyTimes();
    EasyMock.replay(seekableStreamSupervisorIOConfig);
    Assert.assertTrue(autoScalerConfig.isEmpty());
    SupervisorTaskAutoScaler autoscaler4 = spec.createAutoscaler(supervisor4);
    Assert.assertTrue(autoscaler4 instanceof NoopTaskAutoScaler);
  }

  @Test
  public void testAutoScalerReturnsNoopWhenSupervisorIsNotSeekableStreamSupervisor()
  {
    // Test the branch where supervisor instanceof SeekableStreamSupervisor is false
    HashMap<String, Object> autoScalerConfig = new HashMap<>();
    autoScalerConfig.put("enableTaskAutoScaler", true);
    autoScalerConfig.put("taskCountMax", 8);
    autoScalerConfig.put("taskCountMin", 1);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(seekableStreamSupervisorIOConfig.getAutoScalerConfig())
            .andReturn(mapper.convertValue(autoScalerConfig, AutoScalerConfig.class))
            .anyTimes();
    EasyMock.expect(seekableStreamSupervisorIOConfig.getStream()).andReturn("stream").anyTimes();
    EasyMock.replay(seekableStreamSupervisorIOConfig);

    EasyMock.expect(supervisor4.getActiveTaskGroupsCount()).andReturn(0).anyTimes();
    EasyMock.expect(supervisor4.getIoConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.replay(supervisor4);

    TestSeekableStreamSupervisorSpec spec = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        null,
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        "id1"
    );

    // Create a non-SeekableStreamSupervisor mock
    Supervisor nonSeekableStreamSupervisor = EasyMock.mock(Supervisor.class);
    EasyMock.replay(nonSeekableStreamSupervisor);

    // When passing a non-SeekableStreamSupervisor, should return NoopTaskAutoScaler
    SupervisorTaskAutoScaler autoscaler = spec.createAutoscaler(nonSeekableStreamSupervisor);
    Assert.assertTrue(
        "Should return NoopTaskAutoScaler when supervisor is not SeekableStreamSupervisor",
        autoscaler instanceof NoopTaskAutoScaler
    );
  }

  @Test
  public void testAutoScalerReturnsNoopWhenConfigIsNull()
  {
    // Test the branch where autoScalerConfig is null
    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(seekableStreamSupervisorIOConfig.getAutoScalerConfig())
            .andReturn(null)
            .anyTimes();
    EasyMock.expect(seekableStreamSupervisorIOConfig.getStream()).andReturn("stream").anyTimes();
    EasyMock.replay(seekableStreamSupervisorIOConfig);

    EasyMock.expect(supervisor4.getActiveTaskGroupsCount()).andReturn(0).anyTimes();
    EasyMock.expect(supervisor4.getIoConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.replay(supervisor4);

    TestSeekableStreamSupervisorSpec spec = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        null,
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        "id1"
    );

    // When autoScalerConfig is null, should return NoopTaskAutoScaler
    SupervisorTaskAutoScaler autoscaler = spec.createAutoscaler(supervisor4);
    Assert.assertTrue(
        "Should return NoopTaskAutoScaler when autoScalerConfig is null",
        autoscaler instanceof NoopTaskAutoScaler
    );
  }

  @Test
  public void testDefaultAutoScalerConfigCreatedWithDefault()
  {
    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(seekableStreamSupervisorIOConfig.getAutoScalerConfig())
            .andReturn(mapper.convertValue(
                ImmutableMap.of(
                    "lagCollectionIntervalMillis",
                    "1",
                    "enableTaskAutoScaler",
                    true,
                    "taskCountMax",
                    "4",
                    "taskCountMin",
                    "1"
                ), AutoScalerConfig.class
            ))
            .anyTimes();
    EasyMock.expect(seekableStreamSupervisorIOConfig.getStream()).andReturn("stream").anyTimes();
    EasyMock.replay(seekableStreamSupervisorIOConfig);

    EasyMock.expect(supervisor4.getIoConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(supervisor4.getActiveTaskGroupsCount()).andReturn(0).anyTimes();
    EasyMock.replay(supervisor4);

    TestSeekableStreamSupervisorSpec spec = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        null,
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        "id1"
    );
    SupervisorTaskAutoScaler autoscaler = spec.createAutoscaler(supervisor4);
    Assert.assertTrue(autoscaler instanceof LagBasedAutoScaler);
    LagBasedAutoScaler lagBasedAutoScaler = (LagBasedAutoScaler) autoscaler;
    LagBasedAutoScalerConfig lagBasedAutoScalerConfig = lagBasedAutoScaler.getAutoScalerConfig();
    Assert.assertEquals(lagBasedAutoScalerConfig.getLagCollectionIntervalMillis(), 1);
    Assert.assertEquals(lagBasedAutoScalerConfig.getLagCollectionRangeMillis(), 600000);
    Assert.assertEquals(lagBasedAutoScalerConfig.getScaleActionStartDelayMillis(), 300000);
    Assert.assertEquals(lagBasedAutoScalerConfig.getScaleActionPeriodMillis(), 60000);
    Assert.assertEquals(lagBasedAutoScalerConfig.getScaleOutThreshold(), 6000000);
    Assert.assertEquals(lagBasedAutoScalerConfig.getScaleInThreshold(), 1000000);
    Assert.assertEquals(lagBasedAutoScalerConfig.getTaskCountMax(), 4);
    Assert.assertEquals(lagBasedAutoScalerConfig.getTaskCountMin(), 1);
    Assert.assertEquals(lagBasedAutoScalerConfig.getScaleInStep(), 1);
    Assert.assertEquals(lagBasedAutoScalerConfig.getScaleOutStep(), 2);
    Assert.assertEquals(lagBasedAutoScalerConfig.getMinTriggerScaleActionFrequencyMillis(), 600000);
  }

  @Test
  public void testSeekableStreamSupervisorSpecWithScaleOut() throws InterruptedException
  {
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR).anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();

    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(getIOConfig(1, true)).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(taskMaster);

    StubServiceEmitter dynamicActionEmitter = new StubServiceEmitter();

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(10);

    LagBasedAutoScaler autoScaler = new LagBasedAutoScaler(
        supervisor,
        DATASOURCE,
        mapper.convertValue(
            getScaleOutProperties(10),
            LagBasedAutoScalerConfig.class
        ),
        spec,
        dynamicActionEmitter
    );
    supervisor.start();
    autoScaler.start();
    supervisor.runInternal();

    int taskCountBeforeScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(1, taskCountBeforeScaleOut);
    Thread.sleep(1000);
    int taskCountAfterScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(2, taskCountAfterScaleOut);
    Assert.assertTrue(
        dynamicActionEmitter
            .getMetricEvents(SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC)
            .stream()
            .map(metric -> metric.getUserDims().get(SeekableStreamSupervisor.AUTOSCALER_SKIP_REASON_DIMENSION))
            .filter(Objects::nonNull)
            .anyMatch("minTriggerScaleActionFrequencyMillis not elapsed yet"::equals));
    emitter.verifyEmitted(SeekableStreamSupervisor.AUTOSCALER_SCALING_TIME_METRIC, 1);
    autoScaler.reset();
    autoScaler.stop();
  }

  @Test
  public void testSeekableStreamSupervisorSpecWithScaleOutAlreadyAtMax() throws InterruptedException
  {
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR).anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();

    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(getIOConfig(2, true)).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(taskMaster);

    StubServiceEmitter dynamicActionEmitter = new StubServiceEmitter();
    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(10)
    {
      @Override
      public int getActiveTaskGroupsCount()
      {
        return 2;
      }
    };

    LagBasedAutoScaler autoScaler = new LagBasedAutoScaler(
        supervisor,
        DATASOURCE,
        mapper.convertValue(
            getScaleOutProperties(2),
            LagBasedAutoScalerConfig.class
        ),
        spec,
        dynamicActionEmitter
    );
    supervisor.start();
    autoScaler.start();
    supervisor.runInternal();
    Thread.sleep(1000);

    Assert.assertTrue(
        dynamicActionEmitter
            .getMetricEvents(SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC)
            .stream()
            .map(metric -> metric.getUserDims().get(SeekableStreamSupervisor.AUTOSCALER_SKIP_REASON_DIMENSION))
            .filter(Objects::nonNull)
            .anyMatch("Already at max task count"::equals));
    emitter.verifyNotEmitted(SeekableStreamSupervisor.AUTOSCALER_SCALING_TIME_METRIC);

    autoScaler.reset();
    autoScaler.stop();
  }

  @Test
  public void testSeekableStreamSupervisorSpecWithNoScalingOnIdleSupervisor() throws InterruptedException
  {
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR).anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();

    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(getIOConfig(1, true)).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(taskMaster);

    TestSeekableStreamSupervisor supervisor = new StateOverrideTestSeekableStreamSupervisor(
        SupervisorStateManager.BasicState.IDLE,
        3
    );

    LagBasedAutoScaler autoScaler = new LagBasedAutoScaler(
        supervisor,
        DATASOURCE,
        mapper.convertValue(
            getScaleOutProperties(2),
            LagBasedAutoScalerConfig.class
        ),
        spec,
        emitter
    );
    supervisor.start();
    autoScaler.start();
    supervisor.runInternal();
    int taskCountBeforeScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(1, taskCountBeforeScaleOut);
    Thread.sleep(1000);
    int taskCountAfterScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(1, taskCountAfterScaleOut);

    autoScaler.reset();
    autoScaler.stop();
  }

  @Test
  public void testSeekableStreamSupervisorSpecWithScaleOutSmallPartitionNumber() throws InterruptedException
  {
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR).anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();

    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(getIOConfig(1, true)).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(taskMaster);

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(2);
    LagBasedAutoScaler autoScaler = new LagBasedAutoScaler(
        supervisor,
        DATASOURCE,
        mapper.convertValue(
            getScaleOutProperties(3),
            LagBasedAutoScalerConfig.class
        ),
        spec,
        emitter
    );
    supervisor.start();
    autoScaler.start();
    supervisor.runInternal();

    int taskCountBeforeScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(1, taskCountBeforeScaleOut);
    Thread.sleep(1000);

    int taskCountAfterScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(2, taskCountAfterScaleOut);
    emitter.verifyEmitted(SeekableStreamSupervisor.AUTOSCALER_SCALING_TIME_METRIC, 1);

    autoScaler.reset();
    autoScaler.stop();
  }

  @Test
  public void testSeekableStreamSupervisorSpecWithScaleIn() throws InterruptedException
  {
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR).anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();
    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(getIOConfig(2, false)).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(taskMaster);

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(3);
    LagBasedAutoScaler autoScaler = new LagBasedAutoScaler(
        supervisor,
        DATASOURCE,
        mapper.convertValue(
            getScaleInProperties(),
            LagBasedAutoScalerConfig.class
        ),
        spec,
        emitter
    );

    // enable autoscaler so that taskcount config will be ignored and init value of taskCount will use taskCountMin.
    Assert.assertEquals(1, (int) supervisor.getIoConfig().getTaskCount());
    supervisor.getIoConfig().setTaskCount(2);

    supervisor.start();
    autoScaler.start();
    supervisor.runInternal();

    int taskCountBeforeScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(2, taskCountBeforeScaleOut);

    Thread.sleep(1000);
    int taskCountAfterScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(1, taskCountAfterScaleOut);
    emitter.verifyEmitted(SeekableStreamSupervisor.AUTOSCALER_SCALING_TIME_METRIC, 1);

    autoScaler.reset();
    autoScaler.stop();
  }

  @Test
  public void testSeekableStreamSupervisorSpecWithScaleInThresholdGreaterThanPartitions() throws InterruptedException
  {
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR).anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();
    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(getIOConfig(2, false)).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(taskMaster);

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(10);
    Map<String, Object> modifiedScaleInProps = getScaleInProperties();

    modifiedScaleInProps.put("taskCountMax", 20);
    modifiedScaleInProps.put("taskCountMin", 15);

    LagBasedAutoScaler autoScaler = new LagBasedAutoScaler(
        supervisor,
        DATASOURCE,
        mapper.convertValue(
            modifiedScaleInProps,
            LagBasedAutoScalerConfig.class
        ),
        spec,
        emitter
    );

    // enable autoscaler so that taskcount config will be ignored and the init value of taskCount will use taskCountMin.
    Assert.assertEquals(1, (int) supervisor.getIoConfig().getTaskCount());
    supervisor.getIoConfig().setTaskCount(2);

    // When
    supervisor.start();
    autoScaler.start();
    supervisor.runInternal();

    Assert.assertEquals(2, (int) supervisor.getIoConfig().getTaskCount());
    Thread.sleep(2000);
    // Then
    Assert.assertEquals(10, (int) supervisor.getIoConfig().getTaskCount());

    emitter.verifyEmitted(SeekableStreamSupervisor.AUTOSCALER_SCALING_TIME_METRIC, 1);

    autoScaler.reset();
    autoScaler.stop();
  }

  @Test
  public void testSeekableStreamSupervisorSpecWithScaleInAlreadyAtMin() throws InterruptedException
  {
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR).anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();

    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(getIOConfig(1, true)).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(taskMaster);

    StubServiceEmitter dynamicActionEmitter = new StubServiceEmitter();
    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(10)
    {
      @Override
      public int getActiveTaskGroupsCount()
      {
        return 1;
      }
    };

    LagBasedAutoScaler autoScaler = new LagBasedAutoScaler(
        supervisor,
        DATASOURCE,
        mapper.convertValue(
            getScaleInProperties(),
            LagBasedAutoScalerConfig.class
        ),
        spec,
        dynamicActionEmitter
    );
    supervisor.start();
    autoScaler.start();
    supervisor.runInternal();
    Thread.sleep(1000);

    Assert.assertTrue(
        dynamicActionEmitter
            .getMetricEvents(SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC)
            .stream()
            .map(metric -> metric.getUserDims().get(SeekableStreamSupervisor.AUTOSCALER_SKIP_REASON_DIMENSION))
            .filter(Objects::nonNull)
            .anyMatch("Already at min task count"::equals));
    emitter.verifyNotEmitted(SeekableStreamSupervisor.AUTOSCALER_SCALING_TIME_METRIC);

    autoScaler.reset();
    autoScaler.stop();
  }

  @Test
  public void testSeekableStreamSupervisorSpecWithScaleDisable() throws InterruptedException
  {
    SeekableStreamSupervisorIOConfig seekableStreamSupervisorIOConfig = new SeekableStreamSupervisorIOConfig(
        "stream",
        new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), ImmutableMap.of(), false, false, false),
        1,
        1,
        new Period("PT1H"),
        new Period("P1D"),
        new Period("PT30S"),
        false,
        new Period("PT30M"),
        null,
        null,
        null,
        LagAggregator.DEFAULT,
        null,
        null,
        null,
        null
    )
    {
    };
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR).anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();

    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(this.seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(taskMaster);

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(3);
    NoopTaskAutoScaler autoScaler = new NoopTaskAutoScaler();
    supervisor.start();
    autoScaler.start();
    supervisor.runInternal();
    int taskCountBeforeScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(1, taskCountBeforeScaleOut);
    Thread.sleep(1 * 1000);
    int taskCountAfterScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(1, taskCountAfterScaleOut);

    autoScaler.reset();
    autoScaler.stop();
  }

  @Test
  public void testEnablingIdleBeviourPerSupervisorWithOverlordConfigEnabled()
  {
    SeekableStreamSupervisorIOConfig seekableStreamSupervisorIOConfig = new SeekableStreamSupervisorIOConfig(
        "stream",
        new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), ImmutableMap.of(), false, false, false),
        1,
        1,
        new Period("PT1H"),
        new Period("P1D"),
        new Period("PT30S"),
        false,
        new Period("PT30M"),
        null,
        null,
        null,
        LagAggregator.DEFAULT,
        null,
        new IdleConfig(true, null),
        null,
        null
    )
    {
    };

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.replay(ingestionSchema);
    EasyMock.expect(dataSchema.getDataSource()).andReturn(DATASOURCE);
    EasyMock.replay(dataSchema);

    spec = new SeekableStreamSupervisorSpec(
        SUPERVISOR,
        ingestionSchema,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        supervisorStateManagerConfig
    )
    {
      @Override
      public Supervisor createSupervisor()
      {
        return null;
      }

      @Override
      protected SeekableStreamSupervisorSpec toggleSuspend(boolean suspend)
      {
        return null;
      }

      @Override
      public String getType()
      {
        return null;
      }

      @Override
      public String getSource()
      {
        return null;
      }
    };

    Assert.assertTrue(Objects.requireNonNull(spec.getIoConfig().getIdleConfig()).isEnabled());
  }

  @Test
  public void testGetContextVauleWithNullContextShouldReturnNull()
  {
    mockIngestionSchema();
    TestSeekableStreamSupervisorSpec spec = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        null,
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        "id1"
    );
    Assert.assertNull(spec.getContextValue("key"));
  }

  @Test
  public void testGetContextVauleForNonExistentKeyShouldReturnNull()
  {
    mockIngestionSchema();
    TestSeekableStreamSupervisorSpec spec = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        ImmutableMap.of("key", "value"),
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        "id1"
    );
    Assert.assertNull(spec.getContextValue("key_not_exists"));
  }

  @Test
  public void testSupervisorIdEqualsDataSourceIfNull()
  {
    mockIngestionSchema();
    TestSeekableStreamSupervisorSpec spec = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        ImmutableMap.of("key", "value"),
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        SUPERVISOR
    );
    Assert.assertEquals(SUPERVISOR, spec.getId());
  }

  @Test
  public void testSupervisorIdDifferentFromDataSource()
  {
    mockIngestionSchema();
    TestSeekableStreamSupervisorSpec spec = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        ImmutableMap.of("key", "value"),
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        SUPERVISOR
    );
    Assert.assertEquals(SUPERVISOR, spec.getId());
  }

  @Test
  public void testGetContextVauleForKeyShouldReturnValue()
  {
    mockIngestionSchema();
    TestSeekableStreamSupervisorSpec spec = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        ImmutableMap.of("key", "value"),
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        "id1"
    );
    Assert.assertEquals("value", spec.getContextValue("key"));
  }

  @Test
  public void test_validateSpecUpdateTo_ShortCircuits()
  {
    mockIngestionSchema();
    TestSeekableStreamSupervisorSpec originalSpec = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        Map.of("key", "value"),
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        "id1"
    );
    TestSeekableStreamSupervisorSpec proposedSpec = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        Map.of("key", "value"),
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        "id1"
    );
    MatcherAssert.assertThat(
        assertThrows(DruidException.class, () -> originalSpec.validateSpecUpdateTo(proposedSpec)),
        new DruidExceptionMatcher(
            DruidException.Persona.USER,
            DruidException.Category.INVALID_INPUT,
            "invalidInput"
        ).expectMessageIs(
            "Cannot update supervisor spec since one or both of the specs have not provided an input source stream in the 'ioConfig'."
        )
    );


    TestSupervisorSpec otherSpec = new TestSupervisorSpec("fake", new Object());
    MatcherAssert.assertThat(
        assertThrows(DruidException.class, () -> originalSpec.validateSpecUpdateTo(otherSpec)),
        new DruidExceptionMatcher(
            DruidException.Persona.USER,
            DruidException.Category.INVALID_INPUT,
            "invalidInput"
        ).expectMessageIs(
            StringUtils.format(
                "Cannot update supervisor spec from type[%s] to type[%s]",
                proposedSpec.getClass().getSimpleName(),
                otherSpec.getClass().getSimpleName()
            )
        )
    );
  }

  @Test
  public void test_validateSpecUpdateTo_SourceStringComparisons()
  {
    mockIngestionSchema();
    TestSeekableStreamSupervisorSpec originalSpec = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        ImmutableMap.of("key", "value"),
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        "id1"
    )
    {
      @Override
      public String getSource()
      {
        return "source1";
      }
    };
    TestSeekableStreamSupervisorSpec proposedSpecDiffSource = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        ImmutableMap.of("key", "value"),
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        "id1"
    )
    {
      @Override
      public String getSource()
      {
        return "source2";
      }
    };
    TestSeekableStreamSupervisorSpec proposedSpecSameSource = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        ImmutableMap.of("key", "value"),
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        "id1"
    )
    {
      @Override
      public String getSource()
      {
        return "source1";
      }
    };

    // Mistmatched stream strings test
    MatcherAssert.assertThat(
        assertThrows(DruidException.class, () -> originalSpec.validateSpecUpdateTo(proposedSpecDiffSource)),
        new DruidExceptionMatcher(
            DruidException.Persona.USER,
            DruidException.Category.INVALID_INPUT,
            "invalidInput"
        ).expectMessageIs(
            "Update of the input source stream from [source1] to [source2] is not supported for a running supervisor."
            + "\nTo perform the update safely, follow these steps:"
            + "\n(1) Suspend this supervisor, reset its offsets and then terminate it. "
            + "\n(2) Create a new supervisor with the new input source stream."
            + "\nNote that doing the reset can cause data duplication or loss if any topic used in the old supervisor is included in the new one too."
        )
    );

    // Happy path test
    originalSpec.validateSpecUpdateTo(proposedSpecSameSource);
  }

  @Test
  public void test_dynamicAllocationNotice_skipsScalingAndEmitsReason_ifTasksArePublishing() throws InterruptedException
  {
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR).anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();
    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(getIOConfig(1, true)).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(taskMaster);

    StubServiceEmitter dynamicActionEmitter = new StubServiceEmitter();
    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(10);

    LagBasedAutoScaler autoScaler = new LagBasedAutoScaler(
        supervisor,
        DATASOURCE,
        mapper.convertValue(
            getScaleOutProperties(2),
            LagBasedAutoScalerConfig.class
        ),
        spec,
        dynamicActionEmitter
    );

    supervisor.addTaskGroupToPendingCompletionTaskGroup(
        0,
        ImmutableMap.of("0", "0"),
        null,
        null,
        Set.of("dummyTask"),
        Collections.emptySet()
    );

    supervisor.start();
    autoScaler.start();

    supervisor.runInternal();
    Thread.sleep(1000); // ensure a dynamic allocation notice completes

    Assert.assertEquals(1, supervisor.getIoConfig().getTaskCount().intValue());
    Assert.assertTrue(
        dynamicActionEmitter
            .getMetricEvents(SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC)
            .stream()
            .map(metric -> metric.getUserDims().get(SeekableStreamSupervisor.AUTOSCALER_SKIP_REASON_DIMENSION))
            .filter(Objects::nonNull)
            .anyMatch("There are tasks pending completion"::equals)
    );

    emitter.verifyNotEmitted(SeekableStreamSupervisor.AUTOSCALER_SCALING_TIME_METRIC);
    autoScaler.reset();
    autoScaler.stop();
  }

  @Test
  public void test_dynamicAllocationNotice_skips_whenSupervisorSuspended() throws InterruptedException
  {
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR).anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();

    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(getIOConfig(1, true)).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    // Suspended → DynamicAllocationTasksNotice should return early and not scale
    EasyMock.expect(spec.isSuspended()).andReturn(true).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(taskMaster);

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(3);
    LagBasedAutoScaler autoScaler = new LagBasedAutoScaler(
        supervisor,
        DATASOURCE,
        mapper.convertValue(
            getScaleOutProperties(2),
            LagBasedAutoScalerConfig.class
        ),
        spec,
        emitter
    );

    supervisor.start();
    autoScaler.start();
    supervisor.runInternal();

    int before = supervisor.getIoConfig().getTaskCount();
    Thread.sleep(1000);
    int after = supervisor.getIoConfig().getTaskCount();
    // No scaling expected because supervisor is suspended
    Assert.assertEquals(before, after);

    autoScaler.reset();
    autoScaler.stop();
  }

  @Test
  public void test_changeTaskCountInIOConfig_handlesExceptionAndStillUpdatesTaskCount() throws InterruptedException
  {
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR).anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();

    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(getIOConfig(1, true)).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    // SupervisorManager present but metadata insert fails → should be handled
    SupervisorManager sm = EasyMock.createMock(SupervisorManager.class);
    MetadataSupervisorManager msm = EasyMock.createMock(MetadataSupervisorManager.class);
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(sm)).anyTimes();
    EasyMock.expect(sm.getMetadataSupervisorManager()).andReturn(msm).anyTimes();
    msm.insert(EasyMock.anyString(), EasyMock.anyObject());
    EasyMock.expectLastCall().andThrow(new RuntimeException("boom")).anyTimes();
    EasyMock.replay(taskMaster, sm, msm);

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(10);
    LagBasedAutoScaler autoScaler = new LagBasedAutoScaler(
        supervisor,
        DATASOURCE,
        mapper.convertValue(
            getScaleOutProperties(2),
            LagBasedAutoScalerConfig.class
        ),
        spec,
        emitter
    );

    supervisor.start();
    autoScaler.start();
    supervisor.runInternal();

    int before = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(1, before);
    Thread.sleep(1000); // allow one dynamic allocation cycle
    int after = supervisor.getIoConfig().getTaskCount();
    // Even though metadata insert failed, taskCount should still be updated in ioConfig
    Assert.assertEquals(2, after);

    autoScaler.reset();
    autoScaler.stop();
  }

  @Test
  public void testMergeSpecConfigs()
  {
    mockIngestionSchema();

    // Given
    // Create existing spec with autoscaler config and taskCount set to 5
    HashMap<String, Object> existingAutoScalerConfig = new HashMap<>();
    existingAutoScalerConfig.put("enableTaskAutoScaler", true);
    existingAutoScalerConfig.put("taskCountMax", 8);
    existingAutoScalerConfig.put("taskCountMin", 1);

    SeekableStreamSupervisorIOConfig existingIoConfig = EasyMock.mock(SeekableStreamSupervisorIOConfig.class);
    EasyMock.expect(existingIoConfig.getAutoScalerConfig())
            .andReturn(mapper.convertValue(existingAutoScalerConfig, AutoScalerConfig.class))
            .anyTimes();
    EasyMock.expect(existingIoConfig.getTaskCount()).andReturn(5).anyTimes();
    EasyMock.replay(existingIoConfig);

    SeekableStreamSupervisorIngestionSpec existingIngestionSchema = EasyMock.mock(SeekableStreamSupervisorIngestionSpec.class);
    EasyMock.expect(existingIngestionSchema.getIOConfig()).andReturn(existingIoConfig).anyTimes();
    EasyMock.expect(existingIngestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(existingIngestionSchema.getTuningConfig())
            .andReturn(seekableStreamSupervisorTuningConfig)
            .anyTimes();
    EasyMock.replay(existingIngestionSchema);

    TestSeekableStreamSupervisorSpec existingSpec = buildDefaultSupervisorSpecWithIngestionSchema(
        "id123",
        existingIngestionSchema
    );

    // Create new spec with autoscaler config that has taskCountStart not set (null) and no taskCount set
    HashMap<String, Object> newAutoScalerConfig = new HashMap<>();
    newAutoScalerConfig.put("enableTaskAutoScaler", true);
    newAutoScalerConfig.put("taskCountMax", 8);
    newAutoScalerConfig.put("taskCountMin", 1);

    SeekableStreamSupervisorIOConfig newIoConfig = EasyMock.mock(SeekableStreamSupervisorIOConfig.class);
    EasyMock.expect(newIoConfig.getAutoScalerConfig())
            .andReturn(mapper.convertValue(newAutoScalerConfig, AutoScalerConfig.class))
            .anyTimes();
    EasyMock.expect(newIoConfig.getTaskCount()).andReturn(null).anyTimes();
    newIoConfig.setTaskCount(5);
    EasyMock.expectLastCall().once();
    EasyMock.replay(newIoConfig);

    SeekableStreamSupervisorIngestionSpec newIngestionSchema = EasyMock.mock(SeekableStreamSupervisorIngestionSpec.class);
    EasyMock.expect(newIngestionSchema.getIOConfig()).andReturn(newIoConfig).anyTimes();
    EasyMock.expect(newIngestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(newIngestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(newIngestionSchema);

    TestSeekableStreamSupervisorSpec newSpec = buildDefaultSupervisorSpecWithIngestionSchema(
        "id124",
        newIngestionSchema
    );

    // Before merge, taskCountStart should be null
    Assert.assertNull(newSpec.getIoConfig().getAutoScalerConfig().getTaskCountStart());

    // When - merge should copy taskCount from existing spec since new spec has no taskCount
    newSpec.merge(existingSpec);

    // Then - verify setTaskCount was called (EasyMock will verify the mock expectations)
    EasyMock.verify(newIoConfig);
  }

  private TestSeekableStreamSupervisorSpec buildDefaultSupervisorSpecWithIngestionSchema(
      String id,
      SeekableStreamSupervisorIngestionSpec ingestionSchema
  )
  {
    return new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        null,
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        id
    );
  }

  private void mockIngestionSchema()
  {
    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(dataSchema.getDataSource()).andReturn(DATASOURCE).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);
    EasyMock.replay(dataSchema);
  }

  private SeekableStreamSupervisorIOConfig getIOConfig(int taskCount, boolean scaleOut)
  {
    if (scaleOut) {
      return new SeekableStreamSupervisorIOConfig(
          "stream",
          new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), ImmutableMap.of(), false, false, false),
          1,
          null, // autoscaler uses taskCountStart/taskCountMin for the initial value
          new Period("PT1H"),
          new Period("P1D"),
          new Period("PT30S"),
          false,
          new Period("PT30M"),
          null,
          null,
          mapper.convertValue(getScaleOutProperties(2), AutoScalerConfig.class),
          LagAggregator.DEFAULT,
          null,
          null,
          null,
          null
      )
      {
      };
    } else {
      return new SeekableStreamSupervisorIOConfig(
          "stream",
          new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), ImmutableMap.of(), false, false, false),
          1,
          null, // autoscaler uses taskCountStart/taskCountMin for the initial value
          new Period("PT1H"),
          new Period("P1D"),
          new Period("PT30S"),
          false,
          new Period("PT30M"),
          null,
          null,
          mapper.convertValue(getScaleInProperties(), AutoScalerConfig.class),
          LagAggregator.DEFAULT,
          null,
          null,
          null,
          null
      )
      {
      };
    }
  }

  private static DataSchema getDataSchema()
  {
    return getDataSchema(DATASOURCE);
  }

  private static Map<String, Object> getScaleOutProperties(int maxTaskCount)
  {
    HashMap<String, Object> autoScalerConfig = new HashMap<>();
    autoScalerConfig.put("enableTaskAutoScaler", true);
    autoScalerConfig.put("lagCollectionIntervalMillis", 50);
    autoScalerConfig.put("lagCollectionRangeMillis", 500);
    autoScalerConfig.put("scaleOutThreshold", 0);
    autoScalerConfig.put("triggerScaleOutFractionThreshold", 0.0);
    autoScalerConfig.put("scaleInThreshold", 1000000);
    autoScalerConfig.put("triggerScaleInFractionThreshold", 0.8);
    autoScalerConfig.put("scaleActionStartDelayMillis", 0);
    autoScalerConfig.put("scaleActionPeriodMillis", 100);
    autoScalerConfig.put("taskCountMax", maxTaskCount);
    autoScalerConfig.put("taskCountMin", 1);
    autoScalerConfig.put("scaleInStep", 1);
    autoScalerConfig.put("scaleOutStep", 2);
    autoScalerConfig.put("minTriggerScaleActionFrequencyMillis", 1200000);
    return autoScalerConfig;
  }

  private static Map<String, Object> getScaleInProperties()
  {
    HashMap<String, Object> autoScalerConfig = new HashMap<>();
    autoScalerConfig.put("enableTaskAutoScaler", true);
    autoScalerConfig.put("lagCollectionIntervalMillis", 500);
    autoScalerConfig.put("lagCollectionRangeMillis", 500);
    autoScalerConfig.put("scaleOutThreshold", 8000000);
    autoScalerConfig.put("triggerScaleOutFractionThreshold", 0.3);
    autoScalerConfig.put("scaleInThreshold", 0);
    autoScalerConfig.put("triggerScaleInFractionThreshold", 0.0);
    autoScalerConfig.put("scaleActionStartDelayMillis", 0);
    autoScalerConfig.put("scaleActionPeriodMillis", 100);
    autoScalerConfig.put("taskCountMax", 2);
    autoScalerConfig.put("taskCountMin", 1);
    autoScalerConfig.put("scaleInStep", 1);
    autoScalerConfig.put("scaleOutStep", 2);
    autoScalerConfig.put("minTriggerScaleActionFrequencyMillis", 1200000);
    return autoScalerConfig;
  }

}
