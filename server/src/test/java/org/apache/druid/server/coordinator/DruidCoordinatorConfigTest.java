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

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.coordinator.config.CoordinatorKillConfigs;
import org.apache.druid.server.coordinator.config.CoordinatorPeriodConfig;
import org.apache.druid.server.coordinator.config.CoordinatorRunConfig;
import org.apache.druid.server.coordinator.config.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.config.HttpLoadQueuePeonConfig;
import org.apache.druid.server.coordinator.config.KillUnusedSegmentsConfig;
import org.apache.druid.server.coordinator.config.MetadataCleanupConfig;
import org.hamcrest.MatcherAssert;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class DruidCoordinatorConfigTest
{

  @Test
  public void testCoordinatorRunConfigDefaultValues()
  {
    final Properties props = new Properties();
    final CoordinatorRunConfig config = deserializeFrom(props, "druid.coordinator", CoordinatorRunConfig.class);

    Assert.assertEquals(Duration.standardMinutes(1), config.getPeriod());
    Assert.assertEquals(Duration.standardMinutes(5), config.getStartDelay());
  }

  @Test
  public void testCoordinatorRunConfigOverrideValues()
  {
    final Properties props = new Properties();
    props.setProperty("druid.coordinator.startDelay", "PT10M");
    props.setProperty("druid.coordinator.period", "PT30S");
    final CoordinatorRunConfig config = deserializeFrom(props, "druid.coordinator", CoordinatorRunConfig.class);

    Assert.assertEquals(Duration.standardSeconds(30), config.getPeriod());
    Assert.assertEquals(Duration.standardMinutes(10), config.getStartDelay());
  }

  @Test
  public void testCoordinatorPeriodConfigDefaultValues()
  {
    final Properties props = new Properties();
    final CoordinatorPeriodConfig config
        = deserializeFrom(props, "druid.coordinator.period", CoordinatorPeriodConfig.class);

    Assert.assertEquals(Duration.standardMinutes(30), config.getIndexingPeriod());
    Assert.assertEquals(Duration.standardMinutes(60), config.getMetadataStoreManagementPeriod());
  }

  @Test
  public void testCoordinatorPeriodConfigOverrideValues()
  {
    final Properties props = new Properties();
    props.setProperty("druid.coordinator.period.indexingPeriod", "PT1M");
    props.setProperty("druid.coordinator.period.metadataStoreManagementPeriod", "PT3M");
    final CoordinatorPeriodConfig config
        = deserializeFrom(props, "druid.coordinator.period", CoordinatorPeriodConfig.class);

    Assert.assertEquals(Duration.standardMinutes(1), config.getIndexingPeriod());
    Assert.assertEquals(Duration.standardMinutes(3), config.getMetadataStoreManagementPeriod());
  }

  @Test
  public void testLoadQueuePeonConfigDefaultValues()
  {
    final Properties props = new Properties();
    final HttpLoadQueuePeonConfig config
        = deserializeFrom(props, "druid.coordinator.loadqueuepeon.http", HttpLoadQueuePeonConfig.class);

    Assert.assertEquals(Duration.standardMinutes(1), config.getRepeatDelay());
    Assert.assertEquals(Duration.standardMinutes(5), config.getHostTimeout());
    Assert.assertEquals(Duration.standardMinutes(15), config.getLoadTimeout());
    Assert.assertEquals(1, config.getBatchSize());
  }

  @Test
  public void testLoadQueuePeonConfigOverrideValues()
  {
    final Properties props = new Properties();
    props.setProperty("druid.coordinator.loadqueuepeon.http.repeatDelay", "PT20M");
    props.setProperty("druid.coordinator.loadqueuepeon.http.hostTimeout", "PT10M");
    props.setProperty("druid.coordinator.loadqueuepeon.http.batchSize", "100");

    final HttpLoadQueuePeonConfig config
        = deserializeFrom(props, "druid.coordinator.loadqueuepeon.http", HttpLoadQueuePeonConfig.class);

    Assert.assertEquals(Duration.standardMinutes(20), config.getRepeatDelay());
    Assert.assertEquals(Duration.standardMinutes(10), config.getHostTimeout());
    Assert.assertEquals(Duration.standardMinutes(15), config.getLoadTimeout());
    Assert.assertEquals(100, config.getBatchSize());
  }

  @Test
  public void testMetadataCleanupConfigDefaultValues()
  {
    final MetadataCleanupConfig config = new MetadataCleanupConfig(null, null, null);

    Assert.assertTrue(config.isCleanupEnabled());
    Assert.assertEquals(Duration.standardDays(1), config.getCleanupPeriod());
    Assert.assertEquals(Duration.standardDays(90), config.getDurationToRetain());
  }

  @Test
  public void testMetadataCleanupConfigOverrideValues()
  {
    final MetadataCleanupConfig config = new MetadataCleanupConfig(
        false,
        Period.parse("PT5H").toStandardDuration(),
        Period.parse("P1D").toStandardDuration()
    );

    Assert.assertFalse(config.isCleanupEnabled());
    Assert.assertEquals(Duration.standardHours(5), config.getCleanupPeriod());
    Assert.assertEquals(Duration.standardHours(24), config.getDurationToRetain());
  }

  @Test
  public void testKillUnusedSegmentsConfigDefaultValues()
  {
    CoordinatorKillConfigs killConfigs = createKillConfig().build();
    final KillUnusedSegmentsConfig config = killConfigs.unusedSegments(null);

    Assert.assertFalse(config.isCleanupEnabled());
    Assert.assertFalse(config.isIgnoreDurationToRetain());
    Assert.assertEquals(Duration.standardDays(1), config.getCleanupPeriod());
    Assert.assertEquals(Duration.standardDays(30), config.getBufferPeriod());
    Assert.assertEquals(Duration.standardDays(90), config.getDurationToRetain());
    Assert.assertEquals(100, config.getMaxSegments());
  }

  @Test
  public void testKillUnusedSegmentsConfigOverrideValues()
  {
    KillUnusedSegmentsConfig inputConfig = new KillUnusedSegmentsConfig(
        true,
        Period.parse("PT30M").toStandardDuration(),
        Period.parse("PT12H").toStandardDuration(),
        true,
        Period.parse("PT60M").toStandardDuration(),
        500
    );
    CoordinatorKillConfigs killConfigs = createKillConfig().unusedSegments(inputConfig).build();

    final KillUnusedSegmentsConfig config = killConfigs.unusedSegments(null);
    Assert.assertTrue(config.isCleanupEnabled());
    Assert.assertTrue(config.isIgnoreDurationToRetain());
    Assert.assertEquals(Duration.standardMinutes(30), config.getCleanupPeriod());
    Assert.assertEquals(Duration.standardMinutes(60), config.getBufferPeriod());
    Assert.assertEquals(Duration.standardHours(12), config.getDurationToRetain());
    Assert.assertEquals(500, config.getMaxSegments());
  }

  @Test
  public void testKillUnusedSegmentsPeriodLessThanIndexingPeriod()
  {
    KillUnusedSegmentsConfig killUnusedConfig
        = KillUnusedSegmentsConfig.builder()
                                  .withCleanupPeriod(Duration.standardSeconds(5))
                                  .build();
    verifyCoordinatorConfigFailsWith(
        createKillConfig().unusedSegments(killUnusedConfig).build(),
        new CoordinatorPeriodConfig(null, Duration.standardSeconds(10)),
        "'druid.coordinator.kill.period'[PT5S] must be greater than or equal to"
        + " 'druid.coordinator.period.indexingPeriod'[PT10S]"
    );
  }

  @Test
  public void testKillUnusedSegmentsMaxSegmentsNegative()
  {
    KillUnusedSegmentsConfig killUnusedConfig
        = KillUnusedSegmentsConfig.builder()
                                  .withMaxSegmentsToKill(-5)
                                  .build();
    verifyCoordinatorConfigFailsWith(
        createKillConfig().unusedSegments(killUnusedConfig).build(),
        new CoordinatorPeriodConfig(null, Duration.standardSeconds(10)),
        "'druid.coordinator.kill.maxSegments'[-5] must be a positive integer."
    );
  }

  @Test
  public void testCoordinatorKillConfigDefaultValues()
  {
    final CoordinatorKillConfigs killConfigs
        = deserializeFrom(new Properties(), "druid.coordinator.kill", CoordinatorKillConfigs.class);

    Assert.assertEquals(MetadataCleanupConfig.DEFAULT, killConfigs.auditLogs());
    Assert.assertEquals(MetadataCleanupConfig.DEFAULT, killConfigs.supervisors());
    Assert.assertEquals(MetadataCleanupConfig.DEFAULT, killConfigs.compactionConfigs());
    Assert.assertEquals(MetadataCleanupConfig.DEFAULT, killConfigs.datasources());
    Assert.assertEquals(MetadataCleanupConfig.DEFAULT, killConfigs.rules());
    Assert.assertEquals(MetadataCleanupConfig.DEFAULT, killConfigs.pendingSegments());
    Assert.assertEquals(MetadataCleanupConfig.DEFAULT, killConfigs.segmentSchemas());
  }

  @Test
  public void testCoordinatorKillConfigOverrideValues()
  {
    final Properties props = new Properties();
    props.setProperty("druid.coordinator.kill.audit.on", "false");
    props.setProperty("druid.coordinator.kill.audit.period", "PT10H");
    props.setProperty("druid.coordinator.kill.audit.durationToRetain", "PT20H");

    props.setProperty("druid.coordinator.kill.compaction.on", "false");
    props.setProperty("druid.coordinator.kill.compaction.period", "PT20H");
    props.setProperty("druid.coordinator.kill.compaction.durationToRetain", "PT30H");

    props.setProperty("druid.coordinator.kill.datasource.on", "false");
    props.setProperty("druid.coordinator.kill.datasource.period", "PT5H");
    props.setProperty("druid.coordinator.kill.datasource.durationToRetain", "PT10H");

    props.setProperty("druid.coordinator.kill.rule.on", "false");
    props.setProperty("druid.coordinator.kill.rule.period", "PT11H");
    props.setProperty("druid.coordinator.kill.rule.durationToRetain", "PT12H");

    props.setProperty("druid.coordinator.kill.supervisor.on", "false");
    props.setProperty("druid.coordinator.kill.supervisor.period", "PT1H");
    props.setProperty("druid.coordinator.kill.supervisor.durationToRetain", "PT2H");

    props.setProperty("druid.coordinator.kill.pendingSegments.on", "false");

    props.setProperty("druid.coordinator.kill.segmentSchema.on", "false");
    props.setProperty("druid.coordinator.kill.segmentSchema.period", "PT2H");
    props.setProperty("druid.coordinator.kill.segmentSchema.durationToRetain", "PT8H");

    final CoordinatorKillConfigs killConfigs
        = deserializeFrom(props, "druid.coordinator.kill", CoordinatorKillConfigs.class);

    Assert.assertEquals(
        new MetadataCleanupConfig(false, Duration.standardHours(10), Duration.standardHours(20)),
        killConfigs.auditLogs()
    );
    Assert.assertEquals(
        new MetadataCleanupConfig(false, Duration.standardHours(20), Duration.standardHours(30)),
        killConfigs.compactionConfigs()
    );
    Assert.assertEquals(
        new MetadataCleanupConfig(false, Duration.standardHours(5), Duration.standardHours(10)),
        killConfigs.datasources()
    );
    Assert.assertEquals(
        new MetadataCleanupConfig(false, Duration.standardHours(11), Duration.standardHours(12)),
        killConfigs.rules()
    );
    Assert.assertEquals(
        new MetadataCleanupConfig(false, Duration.standardHours(1), Duration.standardHours(2)),
        killConfigs.supervisors()
    );
    Assert.assertEquals(
        new MetadataCleanupConfig(false, Duration.standardHours(2), Duration.standardHours(8)),
        killConfigs.segmentSchemas()
    );
    Assert.assertFalse(killConfigs.pendingSegments().isCleanupEnabled());
  }

  @Test
  public void testCoordinatorConfigFailsWhenCleanupPeriodIsInvalid()
  {
    // Validation fails when cleanup period is less than metadata store management period
    final MetadataCleanupConfig cleanupConfig
        = new MetadataCleanupConfig(true, Duration.standardMinutes(30), null);
    final CoordinatorPeriodConfig periodConfig = new CoordinatorPeriodConfig(Duration.standardHours(1), null);

    verifyCoordinatorConfigFailsWith(
        createKillConfig().audit(cleanupConfig).build(),
        periodConfig,
        "'druid.coordinator.kill.audit.period'[PT1800S] must be greater than"
        + " 'druid.coordinator.period.metadataStoreManagementPeriod'[PT3600S]"
    );
    verifyCoordinatorConfigFailsWith(
        createKillConfig().compaction(cleanupConfig).build(),
        periodConfig,
        "'druid.coordinator.kill.compaction.period'[PT1800S] must be greater than"
        + " 'druid.coordinator.period.metadataStoreManagementPeriod'[PT3600S]"
    );
    verifyCoordinatorConfigFailsWith(
        createKillConfig().datasource(cleanupConfig).build(),
        periodConfig,
        "'druid.coordinator.kill.datasource.period'[PT1800S] must be greater than"
        + " 'druid.coordinator.period.metadataStoreManagementPeriod'[PT3600S]"
    );
    verifyCoordinatorConfigFailsWith(
        createKillConfig().rules(cleanupConfig).build(),
        periodConfig,
        "'druid.coordinator.kill.rule.period'[PT1800S] must be greater than"
        + " 'druid.coordinator.period.metadataStoreManagementPeriod'[PT3600S]"
    );
    verifyCoordinatorConfigFailsWith(
        createKillConfig().supervisors(cleanupConfig).build(),
        periodConfig,
        "'druid.coordinator.kill.supervisor.period'[PT1800S] must be greater than"
        + " 'druid.coordinator.period.metadataStoreManagementPeriod'[PT3600S]"
    );
    verifyCoordinatorConfigFailsWith(
        createKillConfig().segmentSchema(cleanupConfig).build(),
        periodConfig,
        "'druid.coordinator.kill.segmentSchema.period'[PT1800S] must be greater than"
        + " 'druid.coordinator.period.metadataStoreManagementPeriod'[PT3600S]"
    );
  }

  @Test
  public void testCoordinatorConfigFailsWhenRetainDurationIsNegative()
  {
    final MetadataCleanupConfig cleanupConfig
        = new MetadataCleanupConfig(true, null, Duration.standardSeconds(1).negated());

    final CoordinatorPeriodConfig defaultPeriodConfig = new CoordinatorPeriodConfig(null, null);
    verifyCoordinatorConfigFailsWith(
        createKillConfig().audit(cleanupConfig).build(),
        defaultPeriodConfig,
        "'druid.coordinator.kill.audit.durationToRetain'[PT-1S] must be 0 milliseconds or higher"
    );
    verifyCoordinatorConfigFailsWith(
        createKillConfig().compaction(cleanupConfig).build(),
        defaultPeriodConfig,
        "'druid.coordinator.kill.compaction.durationToRetain'[PT-1S] must be 0 milliseconds or higher"
    );
    verifyCoordinatorConfigFailsWith(
        createKillConfig().datasource(cleanupConfig).build(),
        defaultPeriodConfig,
        "'druid.coordinator.kill.datasource.durationToRetain'[PT-1S] must be 0 milliseconds or higher"
    );
    verifyCoordinatorConfigFailsWith(
        createKillConfig().rules(cleanupConfig).build(),
        defaultPeriodConfig,
        "'druid.coordinator.kill.rule.durationToRetain'[PT-1S] must be 0 milliseconds or higher"
    );
    verifyCoordinatorConfigFailsWith(
        createKillConfig().supervisors(cleanupConfig).build(),
        defaultPeriodConfig,
        "'druid.coordinator.kill.supervisor.durationToRetain'[PT-1S] must be 0 milliseconds or higher"
    );
    verifyCoordinatorConfigFailsWith(
        createKillConfig().segmentSchema(cleanupConfig).build(),
        defaultPeriodConfig,
        "'druid.coordinator.kill.segmentSchema.durationToRetain'[PT-1S] must be 0 milliseconds or higher"
    );
  }

  @Test
  public void testCoordinatorConfigFailsWhenRetainDurationIsHigherThanCurrentTime()
  {
    final Duration futureRetainDuration = Duration.millis(System.currentTimeMillis()).plus(10_000);
    final MetadataCleanupConfig cleanupConfig = new MetadataCleanupConfig(true, null, futureRetainDuration);
    final CoordinatorPeriodConfig defaultPeriodConfig = new CoordinatorPeriodConfig(null, null);
    verifyCoordinatorConfigFailsWith(
        createKillConfig().audit(cleanupConfig).build(),
        defaultPeriodConfig,
        "'druid.coordinator.kill.audit.durationToRetain'[%s] cannot be greater than current time in milliseconds",
        futureRetainDuration
    );
    verifyCoordinatorConfigFailsWith(
        createKillConfig().compaction(cleanupConfig).build(),
        defaultPeriodConfig,
        "'druid.coordinator.kill.compaction.durationToRetain'[%s] cannot be greater than current time in milliseconds",
        futureRetainDuration
    );
    verifyCoordinatorConfigFailsWith(
        createKillConfig().datasource(cleanupConfig).build(),
        defaultPeriodConfig,
        "'druid.coordinator.kill.datasource.durationToRetain'[%s] cannot be greater than current time in milliseconds",
        futureRetainDuration
    );
    verifyCoordinatorConfigFailsWith(
        createKillConfig().rules(cleanupConfig).build(),
        defaultPeriodConfig,
        "'druid.coordinator.kill.rule.durationToRetain'[%s] cannot be greater than current time in milliseconds",
        futureRetainDuration
    );
    verifyCoordinatorConfigFailsWith(
        createKillConfig().supervisors(cleanupConfig).build(),
        defaultPeriodConfig,
        "'druid.coordinator.kill.supervisor.durationToRetain'[%s] cannot be greater than current time in milliseconds",
        futureRetainDuration
    );
    verifyCoordinatorConfigFailsWith(
        createKillConfig().segmentSchema(cleanupConfig).build(),
        defaultPeriodConfig,
        "'druid.coordinator.kill.segmentSchema.durationToRetain'[%s] cannot be"
        + " greater than current time in milliseconds",
        futureRetainDuration
    );
  }

  @Test
  public void testSomeBasicStuff()
  {
    Duration millis = Duration.millis(1);
    Duration days = Duration.standardDays(1);
    System.out.println(millis.getMillis() + ", " + days);
  }

  private KillConfigBuilder createKillConfig()
  {
    return new KillConfigBuilder();
  }

  private void verifyCoordinatorConfigFailsWith(
      CoordinatorKillConfigs killConfig,
      CoordinatorPeriodConfig periodConfig,
      String expectedMessageFormat,
      Object... args
  )
  {
    DruidException exception = Assert.assertThrows(
        DruidException.class,
        () -> new DruidCoordinatorConfig(
            new CoordinatorRunConfig(null, null),
            periodConfig,
            killConfig,
            null,
            null
        )
    );

    final String expectedMessage = StringUtils.format(expectedMessageFormat, args);
    MatcherAssert.assertThat(
        exception,
        new DruidExceptionMatcher(
            DruidException.Persona.OPERATOR,
            DruidException.Category.INVALID_INPUT,
            "general"
        ).expectMessageIs(expectedMessage)
    );
  }

  private Injector createInjector(String propertyPrefix, Class<?> clazz)
  {
    return GuiceInjectors.makeStartupInjectorWithModules(
        ImmutableList.of(
            binder -> JsonConfigProvider.bind(binder, propertyPrefix, clazz)
        )
    );
  }

  private <T> T deserializeFrom(Properties props, String propertyPrefix, Class<T> type)
  {
    final Injector injector = createInjector(propertyPrefix, type);
    final JsonConfigProvider<T> provider = JsonConfigProvider.of(propertyPrefix, type);
    provider.inject(props, injector.getInstance(JsonConfigurator.class));
    return provider.get();
  }

  private static class KillConfigBuilder
  {
    MetadataCleanupConfig audit;
    MetadataCleanupConfig compaction;
    MetadataCleanupConfig datasource;
    MetadataCleanupConfig rules;
    MetadataCleanupConfig supervisors;
    MetadataCleanupConfig pendingSegments;
    MetadataCleanupConfig segmentSchema;
    KillUnusedSegmentsConfig unusedSegments;

    KillConfigBuilder audit(MetadataCleanupConfig config)
    {
      this.audit = config;
      return this;
    }

    KillConfigBuilder compaction(MetadataCleanupConfig config)
    {
      this.compaction = config;
      return this;
    }

    KillConfigBuilder datasource(MetadataCleanupConfig config)
    {
      this.datasource = config;
      return this;
    }

    KillConfigBuilder rules(MetadataCleanupConfig config)
    {
      this.rules = config;
      return this;
    }

    KillConfigBuilder supervisors(MetadataCleanupConfig config)
    {
      this.supervisors = config;
      return this;
    }

    KillConfigBuilder segmentSchema(MetadataCleanupConfig config)
    {
      this.segmentSchema = config;
      return this;
    }

    KillConfigBuilder unusedSegments(KillUnusedSegmentsConfig config)
    {
      this.unusedSegments = config;
      return this;
    }

    CoordinatorKillConfigs build()
    {
      return new CoordinatorKillConfigs(
          pendingSegments,
          supervisors,
          audit,
          datasource,
          rules,
          compaction,
          segmentSchema,
          unusedSegments == null ? null : unusedSegments.isCleanupEnabled(),
          unusedSegments == null ? null : unusedSegments.getCleanupPeriod(),
          unusedSegments == null ? null : unusedSegments.getDurationToRetain(),
          unusedSegments == null ? null : unusedSegments.isIgnoreDurationToRetain(),
          unusedSegments == null ? null : unusedSegments.getBufferPeriod(),
          unusedSegments == null ? null : unusedSegments.getMaxSegments()
      );
    }
  }
}
