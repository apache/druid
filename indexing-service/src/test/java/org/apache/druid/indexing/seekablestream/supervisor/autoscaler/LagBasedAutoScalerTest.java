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

package org.apache.druid.indexing.seekablestream.supervisor.autoscaler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.AggregateFunction;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;

public class LagBasedAutoScalerTest
{
  private static final String DATASOURCE = "testDataSource";
  private static final String STREAM = "testStream";

  private static final int DEFAULT_MIN_TASK_COUNT = 0;
  private static final int DEFAULT_SCALE_IN = 1;
  private static final int DEFAULT_SCALE_OUT = 1;
  private static final long DEFAULT_SCALE_IN_THRESHOLD = 100L;
  private static final long DEFAULT_SCALE_OUT_THRESHOLD = 1000L;
  private static final double DEFAULT_SCALE_IN_THRESHOLD_PCT = 0.9;
  private static final double DEFAULT_SCALE_OUT_THRESHOLD_PCT = 0.2;

  private StubServiceEmitter emitter;

  @Mock
  private SeekableStreamSupervisor supervisor;

  @Mock
  private SupervisorSpec spec;

  @Mock
  private SeekableStreamSupervisorIOConfig ioConfig;

  private LagBasedAutoScalerConfig defaultConfig;

  @Before
  public void setup()
  {
    MockitoAnnotations.openMocks(this);
    Mockito.when(supervisor.getIoConfig()).thenReturn(ioConfig);
    Mockito.when(ioConfig.getStream()).thenReturn(STREAM);
    Mockito.when(spec.isSuspended()).thenReturn(false);

    defaultConfig = createConfig(false, 10);
    emitter = new StubServiceEmitter();
  }

  private static LagBasedAutoScalerConfig createConfig(boolean useNearestFactorScaling, int partitionCount)
  {
    return createConfig(
        useNearestFactorScaling,
        DEFAULT_MIN_TASK_COUNT,
        partitionCount,
        DEFAULT_SCALE_IN,
        DEFAULT_SCALE_OUT,
        DEFAULT_SCALE_IN_THRESHOLD,
        DEFAULT_SCALE_OUT_THRESHOLD,
        DEFAULT_SCALE_IN_THRESHOLD_PCT,
        DEFAULT_SCALE_OUT_THRESHOLD_PCT
    );
  }

  private static LagBasedAutoScalerConfig createConfig(
      boolean useNearestFactorScaling,
      int taskCountMin,
      int taskCountMax,
      int scaleInStep,
      int scaleOutStep,
      long scaleInThreshold,
      long scaleOutThreshold,
      double scaleInThresholdPct,
      double scaleOutThresholdPct
  )
  {
    return new LagBasedAutoScalerConfig(
        1L,
        10L,
        0L,
        1L,
        scaleOutThreshold,
        scaleInThreshold,
        scaleOutThresholdPct,
        scaleInThresholdPct,
        taskCountMax,
        null,
        taskCountMin,
        scaleInStep,
        scaleOutStep,
        true,
        0L,
        AggregateFunction.SUM,
        useNearestFactorScaling
    );
  }

  private int testStaticAutoScale(
      List<Long> lagValues,
      LagBasedAutoScalerConfig config,
      int currentTaskCount,
      int partitionCount
  )
  {
    Mockito.when(supervisor.getActiveTaskGroupsCount()).thenReturn(currentTaskCount);
    Mockito.when(supervisor.getPartitionCount()).thenReturn(partitionCount);

    final LagBasedAutoScaler autoScaler = new LagBasedAutoScaler(supervisor, DATASOURCE, config, spec, emitter);

    return autoScaler.computeDesiredTaskCount(lagValues);
  }

  @Test
  public void testScaleOut()
  {
    List<Long> lagValues = ImmutableList.of(2000L, 2100L, 1900L, 2200L, 500L);
    int result = testStaticAutoScale(lagValues, defaultConfig, 2, 10);
    Assert.assertEquals("Should scale out by 1 task", 3, result);
    emitter.verifyNotEmitted(SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC);
  }

  @Test
  public void testScaleIn()
  {
    List<Long> lagValues = ImmutableList.of(500L, 50L, 60L, 70L, 80L, 20L, 10L, 10L, 20L, 20L);
    int result = testStaticAutoScale(lagValues, defaultConfig, 3, 10);
    Assert.assertEquals("Should scale in by 1 task", 2, result);
    emitter.verifyNotEmitted(SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC);
  }

  @Test
  public void testScaleOutMax()
  {
    List<Long> lagValues = ImmutableList.of(2000L, 2100L, 1900L, 2200L, 2300L);
    // currentActiveTaskCount == taskCountMax == partitionCount
    int result = testStaticAutoScale(lagValues, createConfig(false, 10), 10, 10);
    Assert.assertEquals("Should not scale as already at max", -1, result);
    emitter.verifyEmitted(
        SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC,
        ImmutableMap.of(
            SeekableStreamSupervisor.AUTOSCALER_SKIP_REASON_DIMENSION, "Already at max task count"
        ),
        1
    );
    emitter.flush();

    // currentActiveTaskCount == taskCountMax < partitionCount
    LagBasedAutoScalerConfig config = createConfig(
        false,
        1,
        8,
        DEFAULT_SCALE_IN,
        DEFAULT_SCALE_OUT,
        DEFAULT_SCALE_IN_THRESHOLD,
        DEFAULT_SCALE_OUT_THRESHOLD,
        DEFAULT_SCALE_IN_THRESHOLD_PCT,
        DEFAULT_SCALE_OUT_THRESHOLD_PCT
    );
    result = testStaticAutoScale(lagValues, config, 8, 10);
    Assert.assertEquals("Should not scale as already at max", -1, result);
    emitter.verifyEmitted(
        SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC,
        ImmutableMap.of(
            SeekableStreamSupervisor.AUTOSCALER_SKIP_REASON_DIMENSION, "Already at max task count"
        ),
        1
    );
    emitter.flush();

    // currentActiveTaskCount < taskCountMax < partitionCount
    result = testStaticAutoScale(lagValues, config, 7, 10);
    Assert.assertEquals("Should scale to max", 8, result);
    emitter.verifyNotEmitted(SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC);
  }

  @Test
  public void testScaleInMin()
  {
    List<Long> lagValues = ImmutableList.of(50L, 60L, 70L, 80L, 90L);

    // min == current task
    int result = testStaticAutoScale(lagValues, defaultConfig, 0, 10);
    Assert.assertEquals("Should scale as not at min", -1, result);
    emitter.verifyEmitted(
        SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC,
        ImmutableMap.of(
            SeekableStreamSupervisor.AUTOSCALER_SKIP_REASON_DIMENSION, "Already at min task count"
        ),
        1
    );
  }

  @Test
  public void testNoScaleNoOp()
  {
    List<Long> lagValues = ImmutableList.of(500L, 600L, 700L, 800L, 900L);
    int result = testStaticAutoScale(lagValues, defaultConfig, 3, 10);
    Assert.assertEquals("Should not scale as lag is between thresholds", -1, result);
    emitter.verifyNotEmitted(SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC);
  }

  @Test
  public void testNotEnoughSamples()
  {
    List<Long> lagValues = ImmutableList.of(2000L, 500L, 600L, 500L, 600L, 400L, 500L);
    int result = testStaticAutoScale(lagValues, defaultConfig, 3, 10);
    Assert.assertEquals("Should not scale as not enough samples above threshold", -1, result);
    emitter.verifyNotEmitted(SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC);
  }

  @Test
  public void testNearestFactorScaleOut()
  {
    List<Long> lagValues = ImmutableList.of(2000L, 2100L, 1900L, 2200L, 500L);
    int result = testStaticAutoScale(lagValues, createConfig(true, 10), 2, 10);
    Assert.assertEquals("Should scale out to next factor (5)", 5, result);
    emitter.verifyNotEmitted(SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC);
  }

  @Test
  public void testNearestFactorScaleIn()
  {
    List<Long> lagValues = ImmutableList.of(500L, 50L, 60L, 70L, 80L, 20L, 10L, 10L, 20L, 20L);
    int result = testStaticAutoScale(lagValues, createConfig(true, 10), 5, 10);
    Assert.assertEquals("Scale down -1", 4, result);
    emitter.verifyNotEmitted(SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC);
  }

  @Test
  public void testNearestFactorScaleOutMax()
  {
    List<Long> lagValues = ImmutableList.of(2000L, 2100L, 1900L, 2200L, 2300L);

    // currentActiveTaskCount == taskCountMax == partitionCount
    int result = testStaticAutoScale(lagValues, createConfig(true, 10), 10, 10);
    Assert.assertEquals("Should not scale as already at max", -1, result);
    emitter.verifyEmitted(
        SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC,
        ImmutableMap.of(
            SeekableStreamSupervisor.AUTOSCALER_SKIP_REASON_DIMENSION, "Already at max task count"
        ),
        1
    );
    emitter.flush();

    // currentActiveTaskCount == taskCountMax < partitionCount
    LagBasedAutoScalerConfig config = createConfig(
        true,
        1,
        8,
        DEFAULT_SCALE_IN,
        DEFAULT_SCALE_OUT,
        DEFAULT_SCALE_IN_THRESHOLD,
        DEFAULT_SCALE_OUT_THRESHOLD,
        DEFAULT_SCALE_IN_THRESHOLD_PCT,
        DEFAULT_SCALE_OUT_THRESHOLD_PCT
    );
    result = testStaticAutoScale(lagValues, config, 8, 10);
    Assert.assertEquals("Should not scale as already at max", -1, result);
    emitter.verifyEmitted(
        SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC,
        ImmutableMap.of(
            SeekableStreamSupervisor.AUTOSCALER_SKIP_REASON_DIMENSION, "Already at max task count"
        ),
        1
    );
    emitter.flush();

    // currentActiveTaskCount < taskCountMax < partitionCount
    result = testStaticAutoScale(lagValues, config, 6, 10);
    Assert.assertEquals("Should scale to max", 8, result);
    emitter.verifyNotEmitted(SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC);
  }

  @Test
  public void testNearestFactorScaleInMin()
  {
    List<Long> lagValues = ImmutableList.of(50L, 60L, 70L, 80L, 90L);
    // min == current task
    int result = testStaticAutoScale(lagValues, createConfig(true, 10), 0, 10);
    Assert.assertEquals("Should scale as not at min", -1, result);
    emitter.verifyEmitted(
        SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC,
        ImmutableMap.of(
            SeekableStreamSupervisor.AUTOSCALER_SKIP_REASON_DIMENSION, "Already at min task count"
        ),
        1
    );
  }

  @Test
  public void testNearestFactorNoScaleNoOp()
  {
    List<Long> lagValues = ImmutableList.of(500L, 600L, 700L, 800L, 900L);
    int result = testStaticAutoScale(lagValues, createConfig(true, 10), 2, 10);
    Assert.assertEquals("Should not scale as lag is between thresholds", -1, result);
    emitter.verifyNotEmitted(SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC);
  }

  @Test
  public void testNearestFactorWithPrimePartitionCount()
  {
    List<Long> lagValues = ImmutableList.of(2000L, 2100L, 1900L, 2200L, 2300L);
    int result = testStaticAutoScale(lagValues, createConfig(true, 11), 3, 11);
    Assert.assertEquals("Should scale out to next factor (11)", 11, result);
    emitter.verifyNotEmitted(SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC);
  }

  @Test
  public void testNearestFactorWithCompositePartitionCount()
  {
    List<Long> lagValues = ImmutableList.of(2000L, 2100L, 1900L, 2200L, 2300L);
    int result = testStaticAutoScale(lagValues, createConfig(true, 10), 7, 10);
    Assert.assertEquals("Should find nearest factor (10)", 10, result);
    emitter.verifyNotEmitted(SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC);
  }

  @Test
  public void testScaleOutDifferentThresholds()
  {
    final List<Long> lagValues = ImmutableList.of(1500L, 1600L, 1700L, 1800L, 1900L);

    LagBasedAutoScalerConfig highThresholdConfig = createConfig(
        false,
        DEFAULT_MIN_TASK_COUNT,
        10,
        DEFAULT_SCALE_IN,
        DEFAULT_SCALE_OUT,
        DEFAULT_SCALE_IN_THRESHOLD,
        2000L,
        DEFAULT_SCALE_IN_THRESHOLD_PCT,
        DEFAULT_SCALE_OUT_THRESHOLD_PCT
    );
    int result = testStaticAutoScale(lagValues, highThresholdConfig, 3, 10);
    Assert.assertEquals("Should not scale out with high threshold", -1, result);

    Mockito.reset();

    LagBasedAutoScalerConfig lowThresholdConfig = createConfig(
        false,
        DEFAULT_MIN_TASK_COUNT,
        10,
        DEFAULT_SCALE_IN,
        DEFAULT_SCALE_OUT,
        DEFAULT_SCALE_IN_THRESHOLD,
        DEFAULT_SCALE_OUT_THRESHOLD,
        DEFAULT_SCALE_IN_THRESHOLD_PCT,
        DEFAULT_SCALE_OUT_THRESHOLD_PCT
    );
    result = testStaticAutoScale(lagValues, lowThresholdConfig, 3, 10);
    Assert.assertEquals("Should scale out with low threshold", 4, result);
  }

  @Test
  public void testScaleInDifferentThresholds()
  {
    final List<Long> lagValues = ImmutableList.of(50L, 60L, 70L, 80L, 90L);

    LagBasedAutoScalerConfig highThresholdConfig = createConfig(
        false,
        DEFAULT_MIN_TASK_COUNT,
        10,
        DEFAULT_SCALE_IN,
        DEFAULT_SCALE_OUT,
        2000L,
        2500L,
        DEFAULT_SCALE_IN_THRESHOLD_PCT,
        DEFAULT_SCALE_OUT_THRESHOLD_PCT
    );
    int result = testStaticAutoScale(lagValues, highThresholdConfig, 3, 10);
    Assert.assertEquals("Should scale in with high threshold", 2, result);

    Mockito.reset();

    LagBasedAutoScalerConfig lowThresholdConfig = createConfig(
        false,
        DEFAULT_MIN_TASK_COUNT,
        10,
        DEFAULT_SCALE_IN,
        DEFAULT_SCALE_OUT,
        20L,
        DEFAULT_SCALE_OUT_THRESHOLD,
        DEFAULT_SCALE_IN_THRESHOLD_PCT,
        DEFAULT_SCALE_OUT_THRESHOLD_PCT
    );
    result = testStaticAutoScale(lagValues, lowThresholdConfig, 3, 10);
    Assert.assertEquals("Should not scale in with low threshold", -1, result);
  }
}
