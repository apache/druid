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

import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.http.client.pool.ResourcePool;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.Validation;
import java.util.Properties;

/**
 * Covers reading {@code druid.broker.http.poolImplementation} out of runtime.properties, which is how an operator
 * moves off the default pooling implementation.
 */
public class DruidHttpClientConfigTest
{
  private static final String PROPERTY_BASE = "druid.broker.http";

  @Test
  public void testPoolImplementationDefaultsToAdaptive()
  {
    Assert.assertEquals(ResourcePool.Implementation.ADAPTIVE, configure(new Properties()).getPoolImplementation());
  }

  @Test
  public void testPoolImplementationCanBeSwitchedBackToRetaining()
  {
    final Properties properties = new Properties();
    properties.setProperty(PROPERTY_BASE + ".poolImplementation", "retaining");

    Assert.assertEquals(ResourcePool.Implementation.RETAINING, configure(properties).getPoolImplementation());
  }

  /**
   * The value an operator writes is not case sensitive, since the enum constant and the property value it is spelled
   * with differ in case.
   */
  @Test
  public void testPoolImplementationIsNotCaseSensitive()
  {
    final Properties properties = new Properties();
    properties.setProperty(PROPERTY_BASE + ".poolImplementation", "RETAINING");

    Assert.assertEquals(ResourcePool.Implementation.RETAINING, configure(properties).getPoolImplementation());
  }

  /**
   * A misspelled implementation fails the service at startup rather than silently leaving the default in place.
   */
  @Test
  public void testUnknownPoolImplementationIsRejected()
  {
    final Properties properties = new Properties();
    properties.setProperty(PROPERTY_BASE + ".poolImplementation", "semaphore");

    Assert.assertThrows(RuntimeException.class, () -> configure(properties));
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
