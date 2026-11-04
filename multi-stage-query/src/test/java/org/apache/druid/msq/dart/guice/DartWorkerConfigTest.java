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

package org.apache.druid.msq.dart.guice;

import com.google.inject.ProvisionException;
import jakarta.validation.Validation;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

/**
 * Tests for reading {@link DartWorkerConfig} from runtime properties, the same way {@link DartWorkerModule} does.
 */
public class DartWorkerConfigTest
{
  private static final String PROPERTY_BASE = DartModules.DART_PROPERTY_BASE + ".worker";

  @Test
  public void test_defaults()
  {
    final DartWorkerConfig config = configure(new Properties());
    Assertions.assertEquals(DartWorkerConfig.AUTO, config.getConcurrentQueries());
    Assertions.assertEquals(0.35, config.getHeapFraction());
    Assertions.assertNull(config.getSegmentLoadAheadCount());
    Assertions.assertEquals(Period.minutes(3), config.getControllerTimeout());
    Assertions.assertEquals(Period.seconds(30), config.getSyncTimeout());
  }

  @Test
  public void test_overrides()
  {
    final Properties properties = new Properties();
    properties.setProperty(PROPERTY_BASE + ".concurrentQueries", "4");
    properties.setProperty(PROPERTY_BASE + ".heapFraction", "0.5");
    properties.setProperty(PROPERTY_BASE + ".segmentLoadAheadCount", "8");
    properties.setProperty(PROPERTY_BASE + ".controllerTimeout", "PT5M");
    properties.setProperty(PROPERTY_BASE + ".syncTimeout", "PT1M");

    final DartWorkerConfig config = configure(properties);
    Assertions.assertEquals(4, config.getConcurrentQueries());
    Assertions.assertEquals(0.5, config.getHeapFraction());
    Assertions.assertEquals(8, config.getSegmentLoadAheadCount());
    Assertions.assertEquals(Period.minutes(5), config.getControllerTimeout());
    Assertions.assertEquals(Period.minutes(1), config.getSyncTimeout());
  }

  @Test
  public void test_controllerTimeoutTooShort()
  {
    final Properties properties = new Properties();
    properties.setProperty(PROPERTY_BASE + ".controllerTimeout", "PT30S");

    final ProvisionException e = Assertions.assertThrows(ProvisionException.class, () -> configure(properties));
    Assertions.assertTrue(e.getMessage().contains("controllerTimeout[PT30S] must be at least"), e.getMessage());
  }

  @Test
  public void test_syncTimeoutNotPositive()
  {
    final Properties properties = new Properties();
    properties.setProperty(PROPERTY_BASE + ".syncTimeout", "PT0S");

    final ProvisionException e = Assertions.assertThrows(ProvisionException.class, () -> configure(properties));
    Assertions.assertTrue(e.getMessage().contains("syncTimeout[PT0S] must be positive"), e.getMessage());
  }

  private static DartWorkerConfig configure(final Properties properties)
  {
    final JsonConfigurator configurator = new JsonConfigurator(
        new DefaultObjectMapper(),
        Validation.buildDefaultValidatorFactory().getValidator()
    );
    final JsonConfigProvider<DartWorkerConfig> provider = JsonConfigProvider.of(PROPERTY_BASE, DartWorkerConfig.class);
    provider.inject(properties, configurator);
    return provider.get();
  }
}
