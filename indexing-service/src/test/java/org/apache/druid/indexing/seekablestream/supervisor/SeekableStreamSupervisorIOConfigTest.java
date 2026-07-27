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

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.AutoScalerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.hamcrest.MatcherAssert;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SeekableStreamSupervisorIOConfigTest
{

  private final Map<Integer, Integer> serverPriorityToReplicas = Map.of(
      1, 2,
      2, 3
  );

  @Test
  public void testAllDefaults()
  {
    LagAggregator lagAggregator = mock(LagAggregator.class);
    InputFormat inputFormat = mock(InputFormat.class);

    SeekableStreamSupervisorIOConfig config = new TestableSeekableStreamSupervisorIOConfig(
        "stream",
        inputFormat,
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
        lagAggregator,
        null,
        null,
        null,
        null,
        null
    )
    {
    };

    Assert.assertEquals("stream", config.getStream());
    Assert.assertEquals(inputFormat, config.getInputFormat());
    Assert.assertEquals(Integer.valueOf(1), config.getReplicas());
    Assert.assertEquals(1, config.getTaskCount());
    Assert.assertEquals(Duration.standardHours(1), config.getTaskDuration());
    Assert.assertEquals(Duration.standardSeconds(5), config.getStartDelay());
    Assert.assertEquals(Duration.standardSeconds(30), config.getPeriod());
    Assert.assertFalse(config.isUseEarliestSequenceNumber());
    Assert.assertEquals(Duration.standardMinutes(30), config.getCompletionTimeout());
    Assert.assertFalse(config.getEarlyMessageRejectionPeriod().isPresent());
    Assert.assertFalse(config.getLateMessageRejectionPeriod().isPresent());
    Assert.assertFalse(config.getLateMessageRejectionStartDateTime().isPresent());
    Assert.assertNull(config.getIdleConfig());
    Assert.assertNull(config.getStopTaskCount());
    Assert.assertEquals(lagAggregator, config.getLagAggregator());
    Assert.assertEquals(1, config.getMaxAllowedStops());
    Assert.assertNull(config.getServerPriorityToReplicas());
  }

  @Test
  public void testTaskCountResolutionInConstructor()
  {
    // Constructor priority is "explicit taskCount > taskCountStart > taskCountMin" so that a
    // previously autoscaled taskCount survives a Jackson round-trip through the metadata store.

    // taskCount=10 + taskCountStart=5 -> taskCount wins, isExplicit=true.
    assertTaskCount(10, autoScaler(5, 3), 10, true);

    // taskCount=null + taskCountStart=5 -> taskCountStart, isExplicit=false.
    assertTaskCount(null, autoScaler(5, 3), 5, false);

    // taskCount=null + no taskCountStart -> taskCountMin, isExplicit=false.
    assertTaskCount(null, autoScaler(null, 3), 3, false);

    // taskCount=10, no autoscaler -> taskCount, isExplicit=true.
    assertTaskCount(10, null, 10, true);
  }

  private static AutoScalerConfig autoScaler(@Nullable Integer taskCountStart, int taskCountMin)
  {
    final AutoScalerConfig config = mock(AutoScalerConfig.class);
    when(config.getEnableTaskAutoScaler()).thenReturn(true);
    when(config.getTaskCountStart()).thenReturn(taskCountStart);
    when(config.getTaskCountMin()).thenReturn(taskCountMin);
    return config;
  }

  private static void assertTaskCount(
      @Nullable Integer taskCount,
      @Nullable AutoScalerConfig autoScalerConfig,
      int expectedTaskCount,
      boolean expectedExplicit
  )
  {
    final SeekableStreamSupervisorIOConfig config = new TestableSeekableStreamSupervisorIOConfig(
        "stream",
        null,
        2,
        taskCount,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        autoScalerConfig,
        mock(LagAggregator.class),
        null,
        null,
        null,
        null,
        null
    )
    {
    };
    Assert.assertEquals(expectedTaskCount, config.getTaskCount());
    Assert.assertEquals(expectedExplicit, config.isTaskCountExplicit());
  }

  @Test
  public void testBothLateMessageRejectionPeriodAndStartDateTime()
  {
    LagAggregator lagAggregator = mock(LagAggregator.class);

    IAE ex = Assert.assertThrows(
        IAE.class,
        () -> new TestableSeekableStreamSupervisorIOConfig(
            "stream",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Period.seconds(10),
            null,
            null,
            lagAggregator,
            DateTimes.nowUtc(),
            null,
            null,
            null,
            null
        )
        {
        }
    );
    Assert.assertTrue(
        ex.getMessage()
          .contains(
              "SeekableStreamSupervisorIOConfig does not support both properties lateMessageRejectionStartDateTime and lateMessageRejectionPeriod"
          )
    );
  }

  @Test
  public void testNullAggregatorThrows()
  {
    DruidException ex = Assert.assertThrows(
        DruidException.class,
        () -> new TestableSeekableStreamSupervisorIOConfig(
            "stream",
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
            null,
            null,
            null,
            null,
            null,
            null,
            null
        )
        {
        }
    );
    Assert.assertTrue(
        ex.getMessage().contains("'lagAggregator' must be specified in supervisor 'spec.ioConfig'")
    );
  }

  @Test
  public void testGetMaxAllowedStopsScalingDisabled()
  {
    LagAggregator lagAggregator = mock(LagAggregator.class);

    // Autoscaler disabled, stopTaskCount unset
    SeekableStreamSupervisorIOConfig config1 = new TestableSeekableStreamSupervisorIOConfig(
        "stream",
        null,
        null,
        7,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        lagAggregator,
        null,
        null,
        null,
        null,
        null
    )
    {
    };
    Assert.assertEquals(7, config1.getMaxAllowedStops());

    // Autoscaler disabled, stopTaskCount set
    SeekableStreamSupervisorIOConfig config2 = new TestableSeekableStreamSupervisorIOConfig(
        "stream",
        null,
        null,
        7,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        lagAggregator,
        null,
        null,
        3,
        null,
        null
    )
    {
    };
    Assert.assertEquals(3, config2.getMaxAllowedStops());
  }

  @Test
  public void testGetMaxAllowedStopsScalingEnabled()
  {
    LagAggregator lagAggregator = mock(LagAggregator.class);

    AutoScalerConfig autoScalerConfig = mock(AutoScalerConfig.class);

    // Autoscaler enabled, stopTaskCountRatio set
    when(autoScalerConfig.getEnableTaskAutoScaler()).thenReturn(true);
    when(autoScalerConfig.getTaskCountStart()).thenReturn(10);
    when(autoScalerConfig.getStopTaskCountRatio()).thenReturn(0.5);

    SeekableStreamSupervisorIOConfig config = new TestableSeekableStreamSupervisorIOConfig(
        "stream",
        null,
        null,
        10,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        autoScalerConfig,
        lagAggregator,
        null,
        null,
        1,
        null,
        null
    )
    {
    };

    Assert.assertEquals(5, config.getMaxAllowedStops());

    // Ensure never goes below 1
    when(autoScalerConfig.getStopTaskCountRatio()).thenReturn(0.05);
    Assert.assertEquals(1, config.getMaxAllowedStops());

    // Autoscaler enabled, stopTaskCountRatio unset, stopTaskCount set
    when(autoScalerConfig.getEnableTaskAutoScaler()).thenReturn(true);
    when(autoScalerConfig.getTaskCountStart()).thenReturn(10);
    when(autoScalerConfig.getStopTaskCountRatio()).thenReturn(null);

    SeekableStreamSupervisorIOConfig config2 = new TestableSeekableStreamSupervisorIOConfig(
        "stream",
        null,
        null,
        10,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        autoScalerConfig,
        lagAggregator,
        null,
        null,
        1,
        null,
        null
    )
    {
    };

    Assert.assertEquals(1, config2.getMaxAllowedStops());


    // Autoscaler enabled, stopTaskCountRatio unset, stopTaskCount unset
    when(autoScalerConfig.getEnableTaskAutoScaler()).thenReturn(true);
    when(autoScalerConfig.getTaskCountStart()).thenReturn(10);
    when(autoScalerConfig.getStopTaskCountRatio()).thenReturn(null);

    SeekableStreamSupervisorIOConfig config3 = new TestableSeekableStreamSupervisorIOConfig(
        "stream",
        null,
        null,
        10,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        autoScalerConfig,
        lagAggregator,
        null,
        null,
        null,
        null,
        null
    )
    {
    };

    Assert.assertEquals(10, config3.getMaxAllowedStops());
  }

  @Test
  public void testReplicasIsSetWhenserverPriorityToReplicas()
  {
    final SeekableStreamSupervisorIOConfig config = makeSeekableStreamSupervisorIOConfig(null, serverPriorityToReplicas);
    Assert.assertEquals(serverPriorityToReplicas, config.getServerPriorityToReplicas());
    Assert.assertEquals(Integer.valueOf(5), config.getReplicas());
  }

  @Test
  public void testReplicasOnlyConfig()
  {
    final SeekableStreamSupervisorIOConfig config = makeSeekableStreamSupervisorIOConfig(4, null);
    Assert.assertEquals(Integer.valueOf(4), config.getReplicas());
    Assert.assertNull(config.getServerPriorityToReplicas());
  }

  @Test
  public void testMatchingReplicasAndServerPriority()
  {
    final SeekableStreamSupervisorIOConfig config = makeSeekableStreamSupervisorIOConfig(5, serverPriorityToReplicas);
    Assert.assertEquals(Integer.valueOf(5), config.getReplicas());
    Assert.assertEquals(serverPriorityToReplicas, config.getServerPriorityToReplicas());
  }

  @Test
  public void testMismatchBetweenReplicasAndServerPriorityReplicasThrowsException()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> makeSeekableStreamSupervisorIOConfig(3, serverPriorityToReplicas)
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            StringUtils.format(
                "Configured replicas[3] does not match the sum of replicas[5] specified in serverPriorityToReplicas[%s]."
                + " To avoid ambiguity, consider removing [ioConfig.replicas] in favor of [ioConfig.serverPriorityToReplicas].",
                serverPriorityToReplicas
            )
        )
    );
  }

  @Test
  public void testNegativeReplicasThrowsException()
  {
    final Map<Integer, Integer> invalidServerPriorityToReplicas = Map.of(0, 2, 1, -1);
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> makeSeekableStreamSupervisorIOConfig(null, invalidServerPriorityToReplicas)
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            StringUtils.format(
                "Found invalid server replica[-1] for priority[1] in serverPriorityToReplicas[%s]. Replicas must be >= 0.",
                invalidServerPriorityToReplicas
            )
        )
    );
  }

  private SeekableStreamSupervisorIOConfig makeSeekableStreamSupervisorIOConfig(@Nullable Integer replicas, @Nullable Map<Integer, Integer> serverPriorityToReplicas)
  {
    return new TestableSeekableStreamSupervisorIOConfig(
        "stream",
        null,
        replicas,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        mock(LagAggregator.class),
        null,
        null,
        null,
        serverPriorityToReplicas,
        null
    )
    {
    };
  }

  @Test
  public void testBoundedModeWithValidConfig()
  {
    Map<String, Integer> startOffsets = Map.of("0", 100, "1", 200);
    Map<String, Integer> endOffsets = Map.of("0", 500, "1", 600);
    BoundedStreamConfig boundedConfig = new BoundedStreamConfig(startOffsets, endOffsets);

    LagAggregator lagAggregator = mock(LagAggregator.class);

    SeekableStreamSupervisorIOConfig config = new TestableSeekableStreamSupervisorIOConfig(
        "stream",
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
        null,
        lagAggregator,
        null,
        null,
        null,
        null,
        boundedConfig
    )
    {
    };

    Assert.assertTrue(config.isBounded());
    Assert.assertNotNull(config.getBoundedStreamConfig());
    Assert.assertEquals(boundedConfig, config.getBoundedStreamConfig());
  }

  @Test
  public void testUnboundedModeByDefault()
  {
    LagAggregator lagAggregator = mock(LagAggregator.class);

    SeekableStreamSupervisorIOConfig config = new TestableSeekableStreamSupervisorIOConfig(
        "stream",
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
        null,
        lagAggregator,
        null,
        null,
        null,
        null,
        null
    )
    {
    };

    Assert.assertFalse(config.isBounded());
    Assert.assertNull(config.getBoundedStreamConfig());
  }

  @Test
  public void testBoundedModeWithNullConfig()
  {
    LagAggregator lagAggregator = mock(LagAggregator.class);

    SeekableStreamSupervisorIOConfig config = new TestableSeekableStreamSupervisorIOConfig(
        "stream",
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
        null,
        lagAggregator,
        null,
        null,
        null,
        null,
        null
    )
    {
    };

    Assert.assertFalse(config.isBounded());
    Assert.assertNull(config.getBoundedStreamConfig());
  }

  private static SupervisorIOConfigBuilder.DefaultSupervisorIOConfigBuilder ioConfigBuilder()
  {
    return new SupervisorIOConfigBuilder.DefaultSupervisorIOConfigBuilder()
        .withStream("stream")
        .withReplicas(1)
        .withTaskCount(2)
        .withTaskDuration(new Period("PT1H"))
        .withLagAggregator(LagAggregator.DEFAULT);
  }

  @Test
  public void testEqualsAndHashCode()
  {
    final SeekableStreamSupervisorIOConfig config = ioConfigBuilder().build();
    Assert.assertEquals(config, ioConfigBuilder().build());
    Assert.assertEquals(config.hashCode(), ioConfigBuilder().build().hashCode());
    Assert.assertNotEquals(config, null);
    Assert.assertNotEquals(config, "not an io config");
    Assert.assertNotEquals(config, ioConfigBuilder().withStream("other").build());
    Assert.assertNotEquals(config, ioConfigBuilder().withReplicas(9).build());
    Assert.assertNotEquals(config, ioConfigBuilder().withTaskCount(9).build());
    Assert.assertNotEquals(config, ioConfigBuilder().withStopTaskCount(7).build());
    Assert.assertNotEquals(config, ioConfigBuilder().withIdleConfig(new IdleConfig(true, 5L)).build());
  }

  @Test
  public void testIdleConfigEqualsAndHashCode()
  {
    EqualsVerifier.forClass(IdleConfig.class).usingGetClass().verify();
  }

  /**
   * Drift guard: the supervisor restart decision is equality-based, so any field omitted from
   * {@code equals} would let a changed spec persist without restarting. EqualsVerifier reflects over the
   * fields and fails automatically on a newly-added unused field — only abstract field types need prefab
   * values. {@code taskCountExplicit}/{@code autoScalerEnabled} are derived hints (ignored); {@code taskCount}
   * is mutable (NONFINAL_FIELDS suppressed).
   */
  @Test
  public void testEqualsContractCoversAllFields()
  {
    EqualsVerifier.forClass(SeekableStreamSupervisorIOConfig.class)
                  .usingGetClass()
                  .withIgnoredFields("taskCountExplicit", "autoScalerEnabled")
                  .suppress(Warning.NONFINAL_FIELDS)
                  .withPrefabValues(InputFormat.class, mock(InputFormat.class), mock(InputFormat.class))
                  .withPrefabValues(AutoScalerConfig.class, mock(AutoScalerConfig.class), mock(AutoScalerConfig.class))
                  .withPrefabValues(LagAggregator.class, mock(LagAggregator.class), mock(LagAggregator.class))
                  .verify();
  }

  @Test
  public void testDefaultLagAggregatorEquals()
  {
    // The default aggregator is a stateless singleton; instance() always returns DEFAULT.
    final LagAggregator aggregator = LagAggregator.DefaultLagAggregator.instance();
    Assert.assertSame(LagAggregator.DEFAULT, aggregator);
    Assert.assertEquals(aggregator, LagAggregator.DefaultLagAggregator.instance());
    Assert.assertNotEquals(aggregator, null);
    Assert.assertNotEquals(aggregator, "not a lag aggregator");
  }

  /**
   * Concrete subclass for tests that exercise the abstract base directly. Implements the now-abstract
   * {@link SeekableStreamSupervisorIOConfig#toBuilder()} via the generic builder.
   */
  private static class TestableSeekableStreamSupervisorIOConfig extends SeekableStreamSupervisorIOConfig
  {
    TestableSeekableStreamSupervisorIOConfig(
        String stream,
        InputFormat inputFormat,
        Integer replicas,
        Integer taskCount,
        Period taskDuration,
        Period startDelay,
        Period period,
        Boolean useEarliestSequenceNumber,
        Period completionTimeout,
        Period lateMessageRejectionPeriod,
        Period earlyMessageRejectionPeriod,
        AutoScalerConfig autoScalerConfig,
        LagAggregator lagAggregator,
        DateTime lateMessageRejectionStartDateTime,
        IdleConfig idleConfig,
        Integer stopTaskCount,
        Map<Integer, Integer> serverPriorityToReplicas,
        BoundedStreamConfig boundedStreamConfig
    )
    {
      super(
          stream,
          inputFormat,
          replicas,
          taskCount,
          taskDuration,
          startDelay,
          period,
          useEarliestSequenceNumber,
          completionTimeout,
          lateMessageRejectionPeriod,
          earlyMessageRejectionPeriod,
          autoScalerConfig,
          lagAggregator,
          lateMessageRejectionStartDateTime,
          idleConfig,
          stopTaskCount,
          serverPriorityToReplicas,
          boundedStreamConfig
      );
    }

    @Override
    public SupervisorIOConfigBuilder<?, ?> toBuilder()
    {
      return new SupervisorIOConfigBuilder.DefaultSupervisorIOConfigBuilder().copyFromBase(this);
    }
  }
}
