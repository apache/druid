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

package org.apache.druid.common.aws;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.utils.RuntimeInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.retries.AdaptiveRetryStrategy;
import software.amazon.awssdk.retries.LegacyRetryStrategy;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.RetryStrategy;

import java.util.Map;
import java.util.stream.Stream;

public class AWSClientConfigTest
{
  private static final ObjectMapper MAPPER = mapperWithRuntimeInfo(new RuntimeInfo());

  /**
   * Binds a property map the way {@code JsonConfigurator} binds {@code druid.s3.*} at startup, so behaviour that only
   * exists during binding - defaults, unset versus explicitly set, rejection of bad values - is exercised here the
   * same way it happens in a running process.
   */
  private static AWSClientConfig bind(Map<String, Object> properties)
  {
    return MAPPER.convertValue(properties, AWSClientConfig.class);
  }

  private static AWSClientConfig bind(Map<String, Object> properties, RuntimeInfo runtimeInfo)
  {
    return mapperWithRuntimeInfo(runtimeInfo).convertValue(properties, AWSClientConfig.class);
  }

  private static ObjectMapper mapperWithRuntimeInfo(RuntimeInfo runtimeInfo)
  {
    return new ObjectMapper().setInjectableValues(
        new InjectableValues.Std().addValue(RuntimeInfo.class, runtimeInfo)
    );
  }

  @Test
  public void testDefaultRetryModeIsStandard()
  {
    final AWSClientConfig config = new AWSClientConfig();

    Assertions.assertEquals(AWSClientConfig.RetryMode.STANDARD, config.getRetryMode());
    Assertions.assertInstanceOf(StandardRetryStrategy.class, config.getRetryStrategy());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("retryModeStrategies")
  public void testEachRetryModeBuildsItsStrategy(
      AWSClientConfig.RetryMode mode,
      Class<? extends RetryStrategy> expected
  )
  {
    Assertions.assertInstanceOf(expected, mode.createStrategy());
  }

  /**
   * Guards {@link #retryModeStrategies} against a mode being added without a strategy expectation.
   */
  @Test
  public void testEveryRetryModeHasAStrategyExpectation()
  {
    Assertions.assertEquals(AWSClientConfig.RetryMode.values().length, retryModeStrategies().count());
  }

  private static Stream<Arguments> retryModeStrategies()
  {
    return Stream.of(
        Arguments.of(AWSClientConfig.RetryMode.STANDARD, StandardRetryStrategy.class),
        Arguments.of(AWSClientConfig.RetryMode.ADAPTIVE, AdaptiveRetryStrategy.class),
        Arguments.of(AWSClientConfig.RetryMode.LEGACY, LegacyRetryStrategy.class)
    );
  }

  @ParameterizedTest
  @ValueSource(strings = {"adaptive", "ADAPTIVE", "Adaptive"})
  public void testRetryModeParsingIsCaseInsensitive(String value)
  {
    Assertions.assertEquals(AWSClientConfig.RetryMode.ADAPTIVE, AWSClientConfig.RetryMode.fromString(value));
  }

  @Test
  public void testRetryModeBindsFromItsProperty()
  {
    Assertions.assertEquals(
        AWSClientConfig.RetryMode.ADAPTIVE,
        bind(Map.of("retryMode", "adaptive")).getRetryMode()
    );
  }

  @Test
  public void testRetryModeSerializesToItsPropertyValue()
  {
    Assertions.assertEquals("adaptive", MAPPER.convertValue(AWSClientConfig.RetryMode.ADAPTIVE, String.class));
  }

  /**
   * Binding the config is the last point at which a bad mode can be reported against the property that set it, so it
   * has to fail here rather than when some client is first built.
   */
  @Test
  public void testUnrecognizedRetryModeIsRejectedWhenConfigIsBound()
  {
    final IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> bind(Map.of("retryMode", "aggressive"))
    );

    final Throwable rootCause = Throwables.getRootCause(e);
    Assertions.assertInstanceOf(IAE.class, rootCause);
    Assertions.assertTrue(rootCause.getMessage().contains("aggressive"));
  }

  @Test
  public void testUnsetAttemptCountLeavesTheCountTheModeDefines()
  {
    final AWSClientConfig config = new AWSClientConfig();

    Assertions.assertNull(config.getMaxAttempts());
    Assertions.assertEquals(
        AWSClientConfig.RetryMode.STANDARD.createStrategy().maxAttempts(),
        config.getRetryStrategy().maxAttempts()
    );
  }

  @Test
  public void testConfiguredAttemptCountIsApplied()
  {
    Assertions.assertEquals(8, bind(Map.of("maxAttempts", 8)).getRetryStrategy().maxAttempts());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("crossRegionAccessBindings")
  public void testCrossRegionAccessResolution(Map<String, Object> properties, boolean expected)
  {
    Assertions.assertEquals(expected, bind(properties).isCrossRegionAccessEnabled());
  }

  private static Stream<Arguments> crossRegionAccessBindings()
  {
    return Stream.of(
        Arguments.of(Map.of(), false),
        Arguments.of(Map.of("crossRegionAccessEnabled", true), true),
        Arguments.of(Map.of("forceGlobalBucketAccessEnabled", true), true),
        // the new property wins whichever way the two disagree
        Arguments.of(Map.of("forceGlobalBucketAccessEnabled", true, "crossRegionAccessEnabled", false), false),
        Arguments.of(Map.of("forceGlobalBucketAccessEnabled", false, "crossRegionAccessEnabled", true), true)
    );
  }

  /**
   * The deprecated property is only ever populated by its own key, so code still reading it cannot be misled by the
   * replacement being set.
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedPropertyStaysUnsetWhenOnlyItsReplacementIsBound()
  {
    Assertions.assertNull(bind(Map.of()).isForceGlobalBucketAccessEnabled());
    Assertions.assertNull(bind(Map.of("crossRegionAccessEnabled", true)).isForceGlobalBucketAccessEnabled());
  }

  @Test
  public void testLegacyMd5DisabledByDefault()
  {
    Assertions.assertFalse(bind(Map.of()).isEnableLegacyMd5());
  }

  @Test
  public void testLegacyMd5CanBeEnabled()
  {
    Assertions.assertTrue(bind(Map.of("enableLegacyMd5", true)).isEnableLegacyMd5());
  }

  @ParameterizedTest(name = "{0} processors -> {1} connections")
  @CsvSource({"8, 50", "32, 128"})
  public void testDefaultMaxConnectionsTakesTheSdkFloorOrFourPerCore(int processors, int expected)
  {
    Assertions.assertEquals(expected, bind(Map.of(), new FixedProcessorsRuntimeInfo(processors)).getMaxConnections());
  }

  @Test
  public void testExplicitMaxConnectionsOverridesDefault()
  {
    Assertions.assertEquals(
        200,
        bind(Map.of("maxConnections", 200), new FixedProcessorsRuntimeInfo(64)).getMaxConnections()
    );
  }

  private static final class FixedProcessorsRuntimeInfo extends RuntimeInfo
  {
    private final int availableProcessors;

    private FixedProcessorsRuntimeInfo(int availableProcessors)
    {
      this.availableProcessors = availableProcessors;
    }

    @Override
    public int getAvailableProcessors()
    {
      return availableProcessors;
    }
  }
}
