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

package org.apache.druid.guice.http;

import jakarta.validation.Validation;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.http.client.pool.ResourcePool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

/**
 * Covers reading the connection pool knobs out of runtime.properties, which is how an operator moves off the default
 * pooling implementation or turns strict connection validation on.
 */
public class DruidHttpClientConfigTest
{
  private static final String PROPERTY_BASE = "druid.broker.http";

  @Test
  public void testPoolImplementationDefaultsToRetaining()
  {
    Assertions.assertEquals(ResourcePool.Implementation.RETAINING, configure(new Properties()).getPoolImplementation());
  }

  @Test
  public void testPoolImplementationCanBeSwitchedToAdaptive()
  {
    final Properties properties = new Properties();
    properties.setProperty(PROPERTY_BASE + ".poolImplementation", "adaptive");

    Assertions.assertEquals(ResourcePool.Implementation.ADAPTIVE, configure(properties).getPoolImplementation());
  }

  /**
   * Either casing is accepted, since the constant is upper case while the value serialises lower case.
   */
  @Test
  public void testPoolImplementationIsNotCaseSensitive()
  {
    final Properties properties = new Properties();
    properties.setProperty(PROPERTY_BASE + ".poolImplementation", "ADAPTIVE");

    Assertions.assertEquals(ResourcePool.Implementation.ADAPTIVE, configure(properties).getPoolImplementation());
  }

  /**
   * A misspelled implementation fails the service at startup rather than silently leaving the default in place.
   */
  @Test
  public void testUnknownPoolImplementationIsRejected()
  {
    final Properties properties = new Properties();
    properties.setProperty(PROPERTY_BASE + ".poolImplementation", "invalid");

    Assertions.assertThrows(RuntimeException.class, () -> configure(properties));
  }

  @Test
  public void testDurationDefaultsPreserveExistingValues()
  {
    final DruidHttpClientConfig config = configure(new Properties());
    Assertions.assertEquals(900_000L, config.getReadTimeout().getMillis());
    Assertions.assertEquals(240_000L, config.getUnusedConnectionTimeout().getMillis());
    Assertions.assertEquals(10_000L, config.getConnectTimeout().getMillis());
    Assertions.assertEquals(500L, config.getClientConnectTimeout());
  }

  @Test
  public void testHumanReadableDurationProperties()
  {
    final Properties properties = new Properties();
    properties.setProperty(PROPERTY_BASE + ".readTimeout", "30s");
    properties.setProperty(PROPERTY_BASE + ".unusedConnectionTimeout", "5s");
    properties.setProperty(PROPERTY_BASE + ".connectTimeout", "2s");
    properties.setProperty(PROPERTY_BASE + ".clientConnectTimeout", "250ms");

    final DruidHttpClientConfig config = configure(properties);
    Assertions.assertEquals(30_000L, config.getReadTimeout().getMillis());
    Assertions.assertEquals(5_000L, config.getUnusedConnectionTimeout().getMillis());
    Assertions.assertEquals(2_000L, config.getConnectTimeout().getMillis());
    Assertions.assertEquals(250L, config.getClientConnectTimeout());
  }

  @Test
  public void testLegacyDurationPropertiesRemainValid()
  {
    final Properties properties = new Properties();
    properties.setProperty(PROPERTY_BASE + ".readTimeout", "PT15M");
    properties.setProperty(PROPERTY_BASE + ".unusedConnectionTimeout", "PT4M");
    properties.setProperty(PROPERTY_BASE + ".connectTimeout", "PT10S");
    properties.setProperty(PROPERTY_BASE + ".clientConnectTimeout", "500");

    final DruidHttpClientConfig config = configure(properties);
    Assertions.assertEquals(900_000L, config.getReadTimeout().getMillis());
    Assertions.assertEquals(240_000L, config.getUnusedConnectionTimeout().getMillis());
    Assertions.assertEquals(10_000L, config.getConnectTimeout().getMillis());
    Assertions.assertEquals(500L, config.getClientConnectTimeout());
  }

  @Test
  public void testClientConnectTimeoutRetainsFullLegacyLongRange()
  {
    final Properties properties = new Properties();
    properties.setProperty(PROPERTY_BASE + ".clientConnectTimeout", String.valueOf(Long.MAX_VALUE));
    Assertions.assertEquals(Long.MAX_VALUE, configure(properties).getClientConnectTimeout());

    properties.setProperty(PROPERTY_BASE + ".clientConnectTimeout", "PT1S");
    Assertions.assertEquals(1_000L, configure(properties).getClientConnectTimeout());
  }

  @Test
  public void testFractionalMillisecondsAreRoundedUp()
  {
    final Properties properties = new Properties();
    properties.setProperty(PROPERTY_BASE + ".clientConnectTimeout", "250us");
    Assertions.assertEquals(1L, configure(properties).getClientConnectTimeout());
  }

  @Test
  public void testInvalidDurationPropertiesAreRejected()
  {
    for (String input : new String[]{"-5s", "1.5h", "P1M", "9999999999d"}) {
      final Properties properties = new Properties();
      properties.setProperty(PROPERTY_BASE + ".readTimeout", input);
      Assertions.assertThrows(RuntimeException.class, () -> configure(properties), input);
    }
  }

  private static DruidHttpClientConfig configure(Properties properties)
  {
    final JsonConfigurator configurator = new JsonConfigurator(
        new DefaultObjectMapper(),
        Validation.buildDefaultValidatorFactory().getValidator()
    );
    final JsonConfigProvider<DruidHttpClientConfig> provider =
        JsonConfigProvider.of(PROPERTY_BASE, DruidHttpClientConfig.class);
    provider.inject(properties, configurator);
    return provider.get();
  }
}
