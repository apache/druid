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

package org.apache.druid.consul.discovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

public class ConsulDiscoveryConfigTest
{
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testDefaultValuesSerde() throws Exception
  {
    testSerde("{\"service\": {\"servicePrefix\": \"druid\"}}\n");
  }

  @Test
  public void testCustomizedValuesSerde() throws Exception
  {
    testSerde(
        "{\n"
        + "  \"connection\": { \"host\": \"consul.example.com\", \"port\": 8600 },\n"
        + "  \"auth\": { \"aclToken\": \"secret-token\" },\n"
        + "  \"service\": { \"servicePrefix\": \"test-druid\", \"datacenter\": \"dc1\",\n"
        + "                \"healthCheckInterval\": \"PT5S\", \"deregisterAfter\": \"PT30S\" },\n"
        + "  \"watch\": { \"watchSeconds\": \"PT30S\", \"maxWatchRetries\": 100, \"watchRetryDelay\": \"PT5S\" }\n"
        + "}\n"
    );
  }

  @Test
  public void testBasicAuthConfigurationSerde() throws Exception
  {
    testSerde(
        "{\n"
        + "  \"auth\": { \"basicAuthUser\": \"admin\", \"basicAuthPassword\": \"secret\" },\n"
        + "  \"service\": { \"servicePrefix\": \"druid\" }\n"
        + "}\n"
    );
  }

  @Test
  public void testBasicAuthWithAllowOverHttpSerde() throws Exception
  {
    testSerde(
        "{\n"
        + "  \"auth\": { \"basicAuthUser\": \"admin\", \"basicAuthPassword\": \"secret\", \"allowBasicAuthOverHttp\": true },\n"
        + "  \"service\": { \"servicePrefix\": \"druid\" }\n"
        + "}\n"
    );
  }

  @Test
  public void testAllowBasicAuthOverHttpDefaultsToFalse() throws Exception
  {
    ConsulDiscoveryConfig config = testSerdeAndReturn(
        "{\n"
        + "  \"auth\": { \"basicAuthUser\": \"admin\", \"basicAuthPassword\": \"secret\" },\n"
        + "  \"service\": { \"servicePrefix\": \"druid\" }\n"
        + "}\n"
    );
    Assert.assertFalse(config.getAuth().getAllowBasicAuthOverHttp());
  }

  @Test
  public void testAllowBasicAuthOverHttpExplicitlySet() throws Exception
  {
    ConsulDiscoveryConfig config = testSerdeAndReturn(
        "{\n"
        + "  \"auth\": { \"basicAuthUser\": \"admin\", \"basicAuthPassword\": \"secret\", \"allowBasicAuthOverHttp\": true },\n"
        + "  \"service\": { \"servicePrefix\": \"druid\" }\n"
        + "}\n"
    );
    Assert.assertTrue(config.getAuth().getAllowBasicAuthOverHttp());
  }

  @Test
  public void testNegativeMaxWatchRetriesMeansUnlimited() throws Exception
  {
    ConsulDiscoveryConfig config = testSerdeAndReturn(
        "{\n"
        + "  \"service\": { \"servicePrefix\": \"druid\" },\n"
        + "  \"watch\": { \"maxWatchRetries\": -1 }\n"
        + "}\n"
    );
    Assert.assertEquals(Long.MAX_VALUE, config.getWatch().getMaxWatchRetries());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullServicePrefixThrows()
  {
    TestUtils.builder().servicePrefix(null).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyServicePrefixThrows()
  {
    TestUtils.builder().servicePrefix("").build();
  }

  @Test
  public void testLeaderRetryOverrides() throws Exception
  {
    ConsulDiscoveryConfig config = testSerdeAndReturn(
        "{\n"
        + "  \"service\": { \"servicePrefix\": \"druid\" },\n"
        + "  \"leader\": { \"leaderMaxErrorRetries\": 5, \"leaderRetryBackoffMax\": \"PT30S\" }\n"
        + "}\n"
    );
    Assert.assertEquals(5L, config.getLeader().getLeaderMaxErrorRetries());
    Assert.assertEquals(Duration.millis(30000), config.getLeader().getLeaderRetryBackoffMax());
  }

  @Test
  public void testSocketTimeoutMustExceedWatchSeconds()
  {
    try {
      TestUtils.builder()
          .servicePrefix("druid")
          .socketTimeout(Duration.millis(5000))
          .watchSeconds(Duration.millis(60000))
          .build();
      Assert.fail("Expected IllegalArgumentException for socketTimeout <= watchSeconds");
    }
    catch (IllegalArgumentException expected) {
      // expected
    }
  }

  @Test
  public void testDefaultSocketTimeoutGreaterThanWatchSeconds()
  {
    ConsulDiscoveryConfig config = TestUtils.builder()
        .servicePrefix("druid")
        .watchSeconds(Duration.millis(60000))
        .build();

    Assert.assertTrue(config.getConnection().getSocketTimeout().isLongerThan(config.getWatch().getWatchSeconds()));
  }

  @Test
  public void testToStringMasksSensitiveData()
  {
    ConsulDiscoveryConfig config = TestUtils.builder()
        .servicePrefix("druid")
        .datacenter("dc1")
        .aclToken("secret-acl-token")
        .basicAuthUser("admin")
        .basicAuthPassword("password")
        .healthCheckInterval(Duration.standardSeconds(10))
        .deregisterAfter(Duration.standardSeconds(35))
        .watchSeconds(Duration.millis(1000))
        .build();

    String toString = config.toString();

    Assert.assertFalse(toString.contains("secret-acl-token"));
    Assert.assertFalse(toString.contains("password"));
    Assert.assertFalse(toString.contains("admin"));
    Assert.assertTrue(toString.contains("*****"));
    Assert.assertTrue(toString.contains("localhost"));
    Assert.assertTrue(toString.contains("druid"));
  }

  @Test
  public void testLeaderSessionTtlDefault() throws Exception
  {
    ConsulDiscoveryConfig config = TestUtils.builder()
        .servicePrefix("druid")
        .healthCheckInterval(Duration.standardSeconds(10))
        .build();

    // Default should be max(45s, 3 * healthCheckInterval)
    Assert.assertEquals(Duration.standardSeconds(45), config.getLeader().getLeaderSessionTtl());
  }

  @Test
  public void testLeaderSessionTtlCustom() throws Exception
  {
    ConsulDiscoveryConfig config = testSerdeAndReturn(
        "{\n"
        + "  \"service\": { \"servicePrefix\": \"druid\" },\n"
        + "  \"leader\": { \"leaderSessionTtl\": \"PT60S\" }\n"
        + "}\n"
    );

    Assert.assertEquals(Duration.standardSeconds(60), config.getLeader().getLeaderSessionTtl());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLeaderSessionTtlTooLow()
  {
    TestUtils.builder()
        .servicePrefix("druid")
        .leaderSessionTtl(Duration.standardSeconds(5))
        .build();
  }

  @Test
  public void testLeaderSessionTtlSerde() throws Exception
  {
    testSerde(
        "{\n"
        + "  \"service\": { \"servicePrefix\": \"druid\" },\n"
        + "  \"leader\": { \"leaderSessionTtl\": \"PT90S\" }\n"
        + "}\n"
    );
  }

  @Test
  public void testLeaderSessionTtlDependsOnHealthCheckInterval() throws Exception
  {
    // With healthCheckInterval = 20s, default leaderSessionTtl should be 60s (3 * 20s)
    ConsulDiscoveryConfig config = TestUtils.builder()
        .servicePrefix("druid")
        .healthCheckInterval(Duration.standardSeconds(20))
        .build();

    // Should be 3 * healthCheckInterval = 60s (greater than minimum 45s)
    Assert.assertEquals(Duration.standardSeconds(60), config.getLeader().getLeaderSessionTtl());
  }

  @Test
  public void testServiceTagsDefensiveCopyAndUnmodifiable()
  {
    java.util.Map<String, String> originalTags = new java.util.LinkedHashMap<>();
    originalTags.put("key1", "value1");

    ConsulDiscoveryConfig.ServiceConfig serviceConfig = new ConsulDiscoveryConfig.ServiceConfig(
        "druid",
        "dc1",
        originalTags,
        Duration.standardSeconds(10),
        Duration.standardSeconds(90)
    );

    // Original map modifications should not affect stored map
    originalTags.put("key1", "mutated");

    Assert.assertEquals("value1", serviceConfig.getServiceTags().get("key1"));

    try {
      serviceConfig.getServiceTags().put("key2", "value2");
      Assert.fail("Expected UnsupportedOperationException when mutating serviceTags");
    }
    catch (UnsupportedOperationException expected) {
      // expected
    }
  }

  private void testSerde(String jsonStr) throws Exception
  {
    ConsulDiscoveryConfig config = jsonMapper.readValue(jsonStr, ConsulDiscoveryConfig.class);
    ConsulDiscoveryConfig roundTrip = jsonMapper.readValue(
        jsonMapper.writeValueAsString(config),
        ConsulDiscoveryConfig.class
    );
    Assert.assertEquals(config, roundTrip);
  }

  private ConsulDiscoveryConfig testSerdeAndReturn(String jsonStr) throws Exception
  {
    ConsulDiscoveryConfig config = jsonMapper.readValue(jsonStr, ConsulDiscoveryConfig.class);
    ConsulDiscoveryConfig roundTrip = jsonMapper.readValue(
        jsonMapper.writeValueAsString(config),
        ConsulDiscoveryConfig.class
    );
    Assert.assertEquals(config, roundTrip);
    return config;
  }
}
