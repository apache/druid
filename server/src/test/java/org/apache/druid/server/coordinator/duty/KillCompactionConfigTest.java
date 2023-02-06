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

package org.apache.druid.server.coordinator.duty;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.ConfigManager;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.TestDruidCoordinatorConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class KillCompactionConfigTest
{
  @Mock
  private DruidCoordinatorRuntimeParams mockDruidCoordinatorRuntimeParams;

  @Mock
  private ServiceEmitter mockServiceEmitter;

  @Mock
  private SqlSegmentsMetadataManager mockSqlSegmentsMetadataManager;

  @Mock
  private JacksonConfigManager mockJacksonConfigManager;

  @Mock
  private MetadataStorageConnector mockConnector;

  @Mock
  private MetadataStorageTablesConfig mockConnectorConfig;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private KillCompactionConfig killCompactionConfig;

  @Before
  public void setup()
  {
    Mockito.when(mockConnectorConfig.getConfigTable()).thenReturn("druid_config");
  }

  @Test
  public void testRunSkipIfLastRunLessThanPeriod()
  {
    TestDruidCoordinatorConfig druidCoordinatorConfig = new TestDruidCoordinatorConfig.Builder()
        .withMetadataStoreManagementPeriod(new Duration("PT5S"))
        .withCoordinatorCompactionKillPeriod(new Duration(Long.MAX_VALUE))
        .withCoordinatorKillMaxSegments(10)
        .withCoordinatorKillIgnoreDurationToRetain(false)
        .build();
    killCompactionConfig = new KillCompactionConfig(
        druidCoordinatorConfig,
        mockSqlSegmentsMetadataManager,
        mockJacksonConfigManager,
        mockConnector,
        mockConnectorConfig
    );
    killCompactionConfig.run(mockDruidCoordinatorRuntimeParams);
    Mockito.verifyNoInteractions(mockSqlSegmentsMetadataManager);
    Mockito.verifyNoInteractions(mockJacksonConfigManager);
    Mockito.verifyNoInteractions(mockServiceEmitter);
  }

  @Test
  public void testConstructorFailIfInvalidPeriod()
  {
    TestDruidCoordinatorConfig druidCoordinatorConfig = new TestDruidCoordinatorConfig.Builder()
        .withMetadataStoreManagementPeriod(new Duration("PT5S"))
        .withCoordinatorCompactionKillPeriod(new Duration("PT3S"))
        .withCoordinatorKillMaxSegments(10)
        .withCoordinatorKillIgnoreDurationToRetain(false)
        .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Coordinator compaction configuration kill period must be >= druid.coordinator.period.metadataStoreManagementPeriod");
    killCompactionConfig = new KillCompactionConfig(
        druidCoordinatorConfig,
        mockSqlSegmentsMetadataManager,
        mockJacksonConfigManager,
        mockConnector,
        mockConnectorConfig
    );
  }


  @Test
  public void testRunDoNothingIfCurrentConfigIsEmpty()
  {
    Mockito.when(mockDruidCoordinatorRuntimeParams.getEmitter()).thenReturn(mockServiceEmitter);
    // Set current compaction config to an empty compaction config
    Mockito.when(mockConnector.lookup(
        ArgumentMatchers.anyString(),
        ArgumentMatchers.eq("name"),
        ArgumentMatchers.eq("payload"),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY))
    ).thenReturn(null);
    Mockito.when(mockJacksonConfigManager.convertByteToConfig(
        ArgumentMatchers.eq(null),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.class),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.empty()))
    ).thenReturn(CoordinatorCompactionConfig.empty());

    TestDruidCoordinatorConfig druidCoordinatorConfig = new TestDruidCoordinatorConfig.Builder()
        .withMetadataStoreManagementPeriod(new Duration("PT5S"))
        .withCoordinatorCompactionKillPeriod(new Duration("PT6S"))
        .withCoordinatorKillMaxSegments(10)
        .withCoordinatorKillIgnoreDurationToRetain(false)
        .build();
    killCompactionConfig = new KillCompactionConfig(
        druidCoordinatorConfig,
        mockSqlSegmentsMetadataManager,
        mockJacksonConfigManager,
        mockConnector,
        mockConnectorConfig
    );
    killCompactionConfig.run(mockDruidCoordinatorRuntimeParams);
    Mockito.verifyNoInteractions(mockSqlSegmentsMetadataManager);
    final ArgumentCaptor<ServiceEventBuilder> emittedEventCaptor = ArgumentCaptor.forClass(ServiceEventBuilder.class);
    Mockito.verify(mockServiceEmitter).emit(emittedEventCaptor.capture());
    Assert.assertEquals(KillCompactionConfig.COUNT_METRIC, emittedEventCaptor.getValue().build(ImmutableMap.of()).toMap().get("metric"));
    Assert.assertEquals(0, emittedEventCaptor.getValue().build(ImmutableMap.of()).toMap().get("value"));
    Mockito.verify(mockJacksonConfigManager).convertByteToConfig(
        ArgumentMatchers.eq(null),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.class),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.empty())
    );
    Mockito.verify(mockConnector).lookup(
        ArgumentMatchers.anyString(),
        ArgumentMatchers.eq("name"),
        ArgumentMatchers.eq("payload"),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY)
    );
    Mockito.verifyNoMoreInteractions(mockJacksonConfigManager);
  }

  @Test
  public void testRunRemoveInactiveDatasourceCompactionConfig()
  {
    String inactiveDatasourceName = "inactive_datasource";
    String activeDatasourceName = "active_datasource";
    DataSourceCompactionConfig inactiveDatasourceConfig = new DataSourceCompactionConfig(
        inactiveDatasourceName,
        null,
        500L,
        null,
        new Period(3600),
        null,
        new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null),
        null,
        null,
        null,
        null,
        ImmutableMap.of("key", "val")
    );

    DataSourceCompactionConfig activeDatasourceConfig = new DataSourceCompactionConfig(
        activeDatasourceName,
        null,
        500L,
        null,
        new Period(3600),
        null,
        new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null),
        null,
        null,
        null,
        null,
        ImmutableMap.of("key", "val")
    );
    CoordinatorCompactionConfig originalCurrentConfig = CoordinatorCompactionConfig.from(ImmutableList.of(inactiveDatasourceConfig, activeDatasourceConfig));
    byte[] originalCurrentConfigBytes = {1, 2, 3};
    Mockito.when(mockConnector.lookup(
        ArgumentMatchers.anyString(),
        ArgumentMatchers.eq("name"),
        ArgumentMatchers.eq("payload"),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY))
    ).thenReturn(originalCurrentConfigBytes);
    Mockito.when(mockJacksonConfigManager.convertByteToConfig(
        ArgumentMatchers.eq(originalCurrentConfigBytes),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.class),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.empty()))
    ).thenReturn(originalCurrentConfig);
    Mockito.when(mockDruidCoordinatorRuntimeParams.getEmitter()).thenReturn(mockServiceEmitter);
    Mockito.when(mockSqlSegmentsMetadataManager.retrieveAllDataSourceNames()).thenReturn(ImmutableSet.of(activeDatasourceName));
    final ArgumentCaptor<byte[]> oldConfigCaptor = ArgumentCaptor.forClass(byte[].class);
    final ArgumentCaptor<CoordinatorCompactionConfig> newConfigCaptor = ArgumentCaptor.forClass(CoordinatorCompactionConfig.class);
    Mockito.when(mockJacksonConfigManager.set(
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
        oldConfigCaptor.capture(),
        newConfigCaptor.capture(),
        ArgumentMatchers.any())
    ).thenReturn(ConfigManager.SetResult.ok());

    TestDruidCoordinatorConfig druidCoordinatorConfig = new TestDruidCoordinatorConfig.Builder()
        .withMetadataStoreManagementPeriod(new Duration("PT5S"))
        .withCoordinatorCompactionKillPeriod(new Duration("PT6S"))
        .withCoordinatorKillMaxSegments(10)
        .withCoordinatorKillIgnoreDurationToRetain(false)
        .build();
    killCompactionConfig = new KillCompactionConfig(
        druidCoordinatorConfig,
        mockSqlSegmentsMetadataManager,
        mockJacksonConfigManager,
        mockConnector,
        mockConnectorConfig
    );
    killCompactionConfig.run(mockDruidCoordinatorRuntimeParams);

    // Verify and Assert
    Assert.assertNotNull(oldConfigCaptor.getValue());
    Assert.assertEquals(oldConfigCaptor.getValue(), originalCurrentConfigBytes);
    Assert.assertNotNull(newConfigCaptor.getValue());
    // The updated config should only contains one compaction config for the active datasource
    Assert.assertEquals(1, newConfigCaptor.getValue().getCompactionConfigs().size());

    Assert.assertEquals(activeDatasourceConfig, newConfigCaptor.getValue().getCompactionConfigs().get(0));
    final ArgumentCaptor<ServiceEventBuilder> emittedEventCaptor = ArgumentCaptor.forClass(ServiceEventBuilder.class);
    Mockito.verify(mockServiceEmitter).emit(emittedEventCaptor.capture());
    Assert.assertEquals(KillCompactionConfig.COUNT_METRIC, emittedEventCaptor.getValue().build(ImmutableMap.of()).toMap().get("metric"));
    // Should delete 1 config
    Assert.assertEquals(1, emittedEventCaptor.getValue().build(ImmutableMap.of()).toMap().get("value"));

    Mockito.verify(mockJacksonConfigManager).convertByteToConfig(
        ArgumentMatchers.eq(originalCurrentConfigBytes),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.class),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.empty())
    );
    Mockito.verify(mockConnector).lookup(
        ArgumentMatchers.anyString(),
        ArgumentMatchers.eq("name"),
        ArgumentMatchers.eq("payload"),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY)
    );
    Mockito.verify(mockJacksonConfigManager).set(
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
        ArgumentMatchers.any(byte[].class),
        ArgumentMatchers.any(CoordinatorCompactionConfig.class),
        ArgumentMatchers.any()
    );
    Mockito.verifyNoMoreInteractions(mockJacksonConfigManager);
    Mockito.verify(mockSqlSegmentsMetadataManager).retrieveAllDataSourceNames();
    Mockito.verifyNoMoreInteractions(mockSqlSegmentsMetadataManager);
  }

  @Test
  public void testRunRetryForRetryableException()
  {
    String inactiveDatasourceName = "inactive_datasource";
    DataSourceCompactionConfig inactiveDatasourceConfig = new DataSourceCompactionConfig(
        inactiveDatasourceName,
        null,
        500L,
        null,
        new Period(3600),
        null,
        new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null),
        null,
        null,
        null,
        null,
        ImmutableMap.of("key", "val")
    );

    CoordinatorCompactionConfig originalCurrentConfig = CoordinatorCompactionConfig.from(ImmutableList.of(inactiveDatasourceConfig));
    byte[] originalCurrentConfigBytes = {1, 2, 3};
    Mockito.when(mockConnector.lookup(
        ArgumentMatchers.anyString(),
        ArgumentMatchers.eq("name"),
        ArgumentMatchers.eq("payload"),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY))
    ).thenReturn(originalCurrentConfigBytes);
    Mockito.when(mockJacksonConfigManager.convertByteToConfig(
        ArgumentMatchers.eq(originalCurrentConfigBytes),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.class),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.empty()))
    ).thenReturn(originalCurrentConfig);
    Mockito.when(mockDruidCoordinatorRuntimeParams.getEmitter()).thenReturn(mockServiceEmitter);
    Mockito.when(mockSqlSegmentsMetadataManager.retrieveAllDataSourceNames()).thenReturn(ImmutableSet.of());
    Mockito.when(mockJacksonConfigManager.set(
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
        ArgumentMatchers.any(byte[].class),
        ArgumentMatchers.any(CoordinatorCompactionConfig.class),
        ArgumentMatchers.any())
    ).thenAnswer(new Answer() {
      private int count = 0;
      @Override
      public Object answer(InvocationOnMock invocation)
      {
        if (count++ < 3) {
          // Return fail result with RetryableException the first three call to updated set
          return ConfigManager.SetResult.fail(new Exception(), true);
        } else {
          // Return success ok on the fourth call to set updated config
          return ConfigManager.SetResult.ok();
        }
      }
    });

    TestDruidCoordinatorConfig druidCoordinatorConfig = new TestDruidCoordinatorConfig.Builder()
        .withMetadataStoreManagementPeriod(new Duration("PT5S"))
        .withCoordinatorCompactionKillPeriod(new Duration("PT6S"))
        .withCoordinatorKillMaxSegments(10)
        .withCoordinatorKillIgnoreDurationToRetain(false)
        .build();
    killCompactionConfig = new KillCompactionConfig(
        druidCoordinatorConfig,
        mockSqlSegmentsMetadataManager,
        mockJacksonConfigManager,
        mockConnector,
        mockConnectorConfig
    );
    killCompactionConfig.run(mockDruidCoordinatorRuntimeParams);

    // Verify and Assert
    final ArgumentCaptor<ServiceEventBuilder> emittedEventCaptor = ArgumentCaptor.forClass(ServiceEventBuilder.class);
    Mockito.verify(mockServiceEmitter).emit(emittedEventCaptor.capture());
    Assert.assertEquals(KillCompactionConfig.COUNT_METRIC, emittedEventCaptor.getValue().build(ImmutableMap.of()).toMap().get("metric"));
    // Should delete 1 config
    Assert.assertEquals(1, emittedEventCaptor.getValue().build(ImmutableMap.of()).toMap().get("value"));

    // Should call convertByteToConfig and lookup (to refresh current compaction config) four times due to RetryableException when failed
    Mockito.verify(mockJacksonConfigManager, Mockito.times(4)).convertByteToConfig(
        ArgumentMatchers.eq(originalCurrentConfigBytes),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.class),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.empty())
    );
    Mockito.verify(mockConnector, Mockito.times(4)).lookup(
        ArgumentMatchers.anyString(),
        ArgumentMatchers.eq("name"),
        ArgumentMatchers.eq("payload"),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY)
    );

    // Should call set (to try set new updated compaction config) four times due to RetryableException when failed
    Mockito.verify(mockJacksonConfigManager, Mockito.times(4)).set(
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
        ArgumentMatchers.any(byte[].class),
        ArgumentMatchers.any(CoordinatorCompactionConfig.class),
        ArgumentMatchers.any()
    );
    Mockito.verifyNoMoreInteractions(mockJacksonConfigManager);
    // Should call retrieveAllDataSourceNames four times due to RetryableException when failed
    Mockito.verify(mockSqlSegmentsMetadataManager, Mockito.times(4)).retrieveAllDataSourceNames();
    Mockito.verifyNoMoreInteractions(mockSqlSegmentsMetadataManager);
  }
}
