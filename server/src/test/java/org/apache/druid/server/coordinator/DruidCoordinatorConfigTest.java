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

package org.apache.druid.server.coordinator;

import org.apache.druid.java.util.common.config.Config;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;
import org.skife.config.ConfigurationObjectFactory;

import java.util.Properties;

public class DruidCoordinatorConfigTest
{
  @Test
  public void testCoordinatorConfigWithDefaults()
  {
    final ConfigurationObjectFactory factory = Config.createFactory(new Properties());
    final DruidCoordinatorConfig config = factory.build(DruidCoordinatorConfig.class);

    Assert.assertEquals(new Duration("PT300s"), config.getCoordinatorStartDelay());
    Assert.assertEquals(new Duration("PT60s"), config.getCoordinatorPeriod());
    Assert.assertEquals(new Duration("PT1800s"), config.getCoordinatorIndexingPeriod());
    Assert.assertEquals(new Duration("PT1800s"), config.getCoordinatorKillPeriod());
    Assert.assertEquals(new Duration("PT7776000s"), config.getCoordinatorKillDurationToRetain());
    Assert.assertEquals(100, config.getCoordinatorKillMaxSegments());
    Assert.assertEquals(new Duration(15 * 60 * 1000), config.getLoadTimeoutDelay());
    Assert.assertFalse(config.getCoordinatorKillIgnoreDurationToRetain());
    Assert.assertEquals("http", config.getLoadQueuePeonType());
  }

  @Test
  public void testCoordinatorConfigWithNonDefaults()
  {
    final Properties props = new Properties();
    props.setProperty("druid.coordinator.startDelay", "PT1s");
    props.setProperty("druid.coordinator.period", "PT1s");
    props.setProperty("druid.coordinator.period.indexingPeriod", "PT1s");
    props.setProperty("druid.coordinator.kill.on", "true");
    props.setProperty("druid.coordinator.kill.period", "PT10s");
    props.setProperty("druid.coordinator.kill.durationToRetain", "PT1s");
    props.setProperty("druid.coordinator.kill.maxSegments", "10000");
    props.setProperty("druid.coordinator.kill.pendingSegments.on", "true");
    props.setProperty("druid.coordinator.load.timeout", "PT1s");
    props.setProperty("druid.coordinator.loadqueuepeon.repeatDelay", "PT0.100s");
    props.setProperty("druid.coordinator.kill.ignoreDurationToRetain", "true");

    final ConfigurationObjectFactory factory = Config.createFactory(props);
    final DruidCoordinatorConfig config = factory.build(DruidCoordinatorConfig.class);

    Assert.assertEquals(new Duration("PT1s"), config.getCoordinatorStartDelay());
    Assert.assertEquals(new Duration("PT1s"), config.getCoordinatorPeriod());
    Assert.assertEquals(new Duration("PT1s"), config.getCoordinatorIndexingPeriod());
    Assert.assertEquals(new Duration("PT10s"), config.getCoordinatorKillPeriod());
    Assert.assertEquals(new Duration("PT1s"), config.getCoordinatorKillDurationToRetain());
    Assert.assertEquals(10000, config.getCoordinatorKillMaxSegments());
    Assert.assertEquals(new Duration("PT1s"), config.getLoadTimeoutDelay());
    Assert.assertTrue(config.getCoordinatorKillIgnoreDurationToRetain());
  }

  @Test
  public void testCoordinatorConfigWithNegativeDurationToRetain()
  {
    final Properties props = new Properties();
    props.setProperty("druid.coordinator.kill.durationToRetain", "PT-1s");

    final ConfigurationObjectFactory factory = Config.createFactory(props);
    final DruidCoordinatorConfig config = factory.build(DruidCoordinatorConfig.class);

    Assert.assertEquals(new Duration("PT-1s"), config.getCoordinatorKillDurationToRetain());
  }
}
