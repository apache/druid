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
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.utils.RuntimeInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.retries.AdaptiveRetryStrategy;
import software.amazon.awssdk.retries.LegacyRetryStrategy;
import software.amazon.awssdk.retries.StandardRetryStrategy;

public class AWSClientConfigTest
{
  private static final ObjectMapper MAPPER = new ObjectMapper().setInjectableValues(
      new InjectableValues.Std().addValue(RuntimeInfo.class, new RuntimeInfo())
  );

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

    Assertions.assertInstanceOf(StandardRetryStrategy.class, config.getRetryStrategy());
  }

  @Test
  public void testDefaultLeavesAttemptCountToTheSdk()
  {
    final AWSClientConfig config = new AWSClientConfig();

    Assertions.assertNull(config.getMaxRetryAttempts());
    Assertions.assertEquals(
        AwsRetryStrategy.standardRetryStrategy().maxAttempts(),
        config.getRetryStrategy().maxAttempts()
    );
  }

  @Test
  public void testAdaptiveRetryModeIsSelectable() throws Exception
  {
    final AWSClientConfig config = MAPPER.readValue("{\"retryMode\": \"adaptive\"}", AWSClientConfig.class);

    Assertions.assertInstanceOf(AdaptiveRetryStrategy.class, config.getRetryStrategy());
  }

  @Test
  public void testLegacyRetryModeIsSelectable() throws Exception
  {
    final AWSClientConfig config = MAPPER.readValue("{\"retryMode\": \"legacy\"}", AWSClientConfig.class);

    Assertions.assertInstanceOf(LegacyRetryStrategy.class, config.getRetryStrategy());
  }

  @Test
  public void testRetryModeIsCaseInsensitive() throws Exception
  {
    final AWSClientConfig config = MAPPER.readValue("{\"retryMode\": \"ADAPTIVE\"}", AWSClientConfig.class);

    Assertions.assertInstanceOf(AdaptiveRetryStrategy.class, config.getRetryStrategy());
  }

  @Test
  public void testConfiguredAttemptCountIsApplied() throws Exception
  {
    final AWSClientConfig config = MAPPER.readValue("{\"maxRetryAttempts\": 8}", AWSClientConfig.class);

    Assertions.assertEquals(8, config.getRetryStrategy().maxAttempts());
  }

  /**
   * Binding the config is the last point at which a bad mode can be reported against the property that set it, so it
   * has to fail here rather than when some client is first built.
   */
  @Test
  public void testUnrecognizedRetryModeIsRejectedWhenConfigIsBound()
  {
    final JsonMappingException e = Assertions.assertThrows(
        JsonMappingException.class,
        () -> MAPPER.readValue("{\"retryMode\": \"aggressive\"}", AWSClientConfig.class)
    );

    Assertions.assertInstanceOf(IAE.class, e.getCause());
    Assertions.assertTrue(e.getCause().getMessage().contains("aggressive"));
  }

  @Test
  public void testRetryModeSerializesToItsPropertyValue() throws Exception
  {
    final AWSClientConfig config = MAPPER.readValue("{\"retryMode\": \"adaptive\"}", AWSClientConfig.class);

    Assertions.assertEquals("adaptive", MAPPER.convertValue(config.getRetryMode(), String.class));
  }

  @Test
  public void testDefaultCrossRegionAccessEnabled() throws Exception
  {
    AWSClientConfig config = MAPPER.readValue("{}", AWSClientConfig.class);
    Assertions.assertNull(config.isForceGlobalBucketAccessEnabled());
    Assertions.assertFalse(config.isCrossRegionAccessEnabled());
  }

  @Test
  public void testCrossRegionAccessEnabledExplicitlySet() throws Exception
  {
    AWSClientConfig config = MAPPER.readValue("{\"crossRegionAccessEnabled\": true}", AWSClientConfig.class);
    Assertions.assertNull(config.isForceGlobalBucketAccessEnabled());
    Assertions.assertTrue(config.isCrossRegionAccessEnabled());
  }

  @Test
  public void testNewConfigTakesPrecedenceOverDeprecatedWhenBothSet() throws Exception
  {
    AWSClientConfig config = MAPPER.readValue(
        "{\"forceGlobalBucketAccessEnabled\": true, \"crossRegionAccessEnabled\": false}",
        AWSClientConfig.class
    );
    Assertions.assertFalse(config.isCrossRegionAccessEnabled());
  }

  @Test
  public void testNewConfigTrueWinsOverDeprecatedFalse() throws Exception
  {
    AWSClientConfig config = MAPPER.readValue(
        "{\"forceGlobalBucketAccessEnabled\": false, \"crossRegionAccessEnabled\": true}",
        AWSClientConfig.class
    );
    Assertions.assertTrue(config.isCrossRegionAccessEnabled());
  }

  @Test
  public void testDeprecatedForceGlobalBucketAccessAloneTrue() throws Exception
  {
    AWSClientConfig config = MAPPER.readValue(
        "{\"forceGlobalBucketAccessEnabled\": true}",
        AWSClientConfig.class
    );
    Assertions.assertTrue(config.isCrossRegionAccessEnabled());
  }

  @Test
  public void testDeprecatedNotSetFallsThroughToCrossRegion() throws Exception
  {
    AWSClientConfig config = MAPPER.readValue(
        "{\"crossRegionAccessEnabled\": true}",
        AWSClientConfig.class
    );
    Assertions.assertNull(config.isForceGlobalBucketAccessEnabled());
    Assertions.assertTrue(config.isCrossRegionAccessEnabled());
  }

  @Test
  public void testDefaultMaxConnectionsKeepsAwsSdkFloorOnSmallHost() throws Exception
  {
    AWSClientConfig config = mapperWithRuntimeInfo(new FixedProcessorsRuntimeInfo(8))
        .readValue("{}", AWSClientConfig.class);
    Assertions.assertEquals(50, config.getMaxConnections());
  }

  @Test
  public void testDefaultMaxConnectionsScalesWithCoresOnLargeHost() throws Exception
  {
    AWSClientConfig config = mapperWithRuntimeInfo(new FixedProcessorsRuntimeInfo(32))
        .readValue("{}", AWSClientConfig.class);
    Assertions.assertEquals(128, config.getMaxConnections());
  }

  @Test
  public void testExplicitMaxConnectionsOverridesDefault() throws Exception
  {
    AWSClientConfig config = mapperWithRuntimeInfo(new FixedProcessorsRuntimeInfo(64))
        .readValue("{\"maxConnections\": 200}", AWSClientConfig.class);
    Assertions.assertEquals(200, config.getMaxConnections());
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
