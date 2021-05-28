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
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.TestDruidCoordinatorConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.Assert;
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

import java.util.concurrent.atomic.AtomicReference;

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

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private KillCompactionConfig killCompactionConfig;

  @Test
  public void testRunSkipIfLastRunLessThanPeriod()
  {
    TestDruidCoordinatorConfig druidCoordinatorConfig = new TestDruidCoordinatorConfig(
        null,
        null,
        null,
        new Duration("PT5S"),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        new Duration(Long.MAX_VALUE),
        null,
        null,
        null,
        null,
        10,
        null
    );
    killCompactionConfig = new KillCompactionConfig(druidCoordinatorConfig, mockSqlSegmentsMetadataManager, mockJacksonConfigManager);
    killCompactionConfig.run(mockDruidCoordinatorRuntimeParams);
    Mockito.verifyZeroInteractions(mockSqlSegmentsMetadataManager);
    Mockito.verifyZeroInteractions(mockJacksonConfigManager);
    Mockito.verifyZeroInteractions(mockServiceEmitter);
  }

  @Test
  public void testConstructorFailIfInvalidPeriod()
  {
    TestDruidCoordinatorConfig druidCoordinatorConfig = new TestDruidCoordinatorConfig(
        null,
        null,
        null,
        new Duration("PT5S"),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        new Duration("PT3S"),
        null,
        null,
        null,
        null,
        10,
        null
    );
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Coordinator compaction configuration kill period must be >= druid.coordinator.period.metadataStoreManagementPeriod");
    killCompactionConfig = new KillCompactionConfig(druidCoordinatorConfig, mockSqlSegmentsMetadataManager, mockJacksonConfigManager);
  }


  @Test
  public void testRunDoNothingIfCurrentConfigIsEmpty()
  {
    Mockito.when(mockDruidCoordinatorRuntimeParams.getEmitter()).thenReturn(mockServiceEmitter);
    // Set current compaction config to an empty compaction config
    Mockito.when(mockJacksonConfigManager.watch(
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.class),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.empty()))
    ).thenReturn(new AtomicReference<>(CoordinatorCompactionConfig.empty()));

    TestDruidCoordinatorConfig druidCoordinatorConfig = new TestDruidCoordinatorConfig(
        null,
        null,
        null,
        new Duration("PT5S"),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        new Duration("PT6S"),
        null,
        null,
        null,
        null,
        10,
        null
    );
    killCompactionConfig = new KillCompactionConfig(druidCoordinatorConfig, mockSqlSegmentsMetadataManager, mockJacksonConfigManager);
    killCompactionConfig.run(mockDruidCoordinatorRuntimeParams);
    Mockito.verifyZeroInteractions(mockSqlSegmentsMetadataManager);
    final ArgumentCaptor<ServiceEventBuilder> emittedEventCaptor = ArgumentCaptor.forClass(ServiceEventBuilder.class);
    Mockito.verify(mockServiceEmitter).emit(emittedEventCaptor.capture());
    Assert.assertEquals(KillCompactionConfig.COUNT_METRIC, emittedEventCaptor.getValue().build(ImmutableMap.of()).toMap().get("metric"));
    Assert.assertEquals(0, emittedEventCaptor.getValue().build(ImmutableMap.of()).toMap().get("value"));
    Mockito.verify(mockJacksonConfigManager).watch(
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.class),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.empty())
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
        new UserCompactionTaskGranularityConfig(Granularities.HOUR, null),
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
        new UserCompactionTaskGranularityConfig(Granularities.HOUR, null),
        null,
        ImmutableMap.of("key", "val")
    );
    CoordinatorCompactionConfig originalCurrentConfig = CoordinatorCompactionConfig.from(ImmutableList.of(inactiveDatasourceConfig, activeDatasourceConfig));
    Mockito.when(mockDruidCoordinatorRuntimeParams.getEmitter()).thenReturn(mockServiceEmitter);
    Mockito.when(mockJacksonConfigManager.watch(
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.class),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.empty()))
    ).thenReturn(new AtomicReference<>(originalCurrentConfig));
    Mockito.when(mockSqlSegmentsMetadataManager.retrieveAllDataSourceNames()).thenReturn(ImmutableSet.of(activeDatasourceName));
    final ArgumentCaptor<CoordinatorCompactionConfig> oldConfigCaptor = ArgumentCaptor.forClass(CoordinatorCompactionConfig.class);
    final ArgumentCaptor<CoordinatorCompactionConfig> newConfigCaptor = ArgumentCaptor.forClass(CoordinatorCompactionConfig.class);
    Mockito.when(mockJacksonConfigManager.set(
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
        oldConfigCaptor.capture(),
        newConfigCaptor.capture(),
        ArgumentMatchers.any())
    ).thenReturn(ConfigManager.SetResult.ok());

    TestDruidCoordinatorConfig druidCoordinatorConfig = new TestDruidCoordinatorConfig(
        null,
        null,
        null,
        new Duration("PT5S"),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        new Duration("PT6S"),
        null,
        null,
        null,
        null,
        10,
        null
    );
    killCompactionConfig = new KillCompactionConfig(druidCoordinatorConfig, mockSqlSegmentsMetadataManager, mockJacksonConfigManager);
    killCompactionConfig.run(mockDruidCoordinatorRuntimeParams);

    // Verify and Assert
    Assert.assertNotNull(oldConfigCaptor.getValue());
    Assert.assertEquals(oldConfigCaptor.getValue(), originalCurrentConfig);
    Assert.assertNotNull(newConfigCaptor.getValue());
    // The updated config should only contains one compaction config for the active datasource
    Assert.assertEquals(1, newConfigCaptor.getValue().getCompactionConfigs().size());

    Assert.assertEquals(activeDatasourceConfig, newConfigCaptor.getValue().getCompactionConfigs().get(0));
    final ArgumentCaptor<ServiceEventBuilder> emittedEventCaptor = ArgumentCaptor.forClass(ServiceEventBuilder.class);
    Mockito.verify(mockServiceEmitter).emit(emittedEventCaptor.capture());
    Assert.assertEquals(KillCompactionConfig.COUNT_METRIC, emittedEventCaptor.getValue().build(ImmutableMap.of()).toMap().get("metric"));
    // Should delete 1 config
    Assert.assertEquals(1, emittedEventCaptor.getValue().build(ImmutableMap.of()).toMap().get("value"));

    Mockito.verify(mockJacksonConfigManager).watch(
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.class),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.empty())
    );
    Mockito.verify(mockJacksonConfigManager).set(
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
        ArgumentMatchers.any(CoordinatorCompactionConfig.class),
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
        new UserCompactionTaskGranularityConfig(Granularities.HOUR, null),
        null,
        ImmutableMap.of("key", "val")
    );

    CoordinatorCompactionConfig originalCurrentConfig = CoordinatorCompactionConfig.from(ImmutableList.of(inactiveDatasourceConfig));
    Mockito.when(mockDruidCoordinatorRuntimeParams.getEmitter()).thenReturn(mockServiceEmitter);
    Mockito.when(mockJacksonConfigManager.watch(
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.class),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.empty()))
    ).thenReturn(new AtomicReference<>(originalCurrentConfig));
    Mockito.when(mockSqlSegmentsMetadataManager.retrieveAllDataSourceNames()).thenReturn(ImmutableSet.of());
    Mockito.when(mockJacksonConfigManager.set(
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
        ArgumentMatchers.any(CoordinatorCompactionConfig.class),
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

    TestDruidCoordinatorConfig druidCoordinatorConfig = new TestDruidCoordinatorConfig(
        null,
        null,
        null,
        new Duration("PT5S"),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        new Duration("PT6S"),
        null,
        null,
        null,
        null,
        10,
        null
    );
    killCompactionConfig = new KillCompactionConfig(druidCoordinatorConfig, mockSqlSegmentsMetadataManager, mockJacksonConfigManager);
    killCompactionConfig.run(mockDruidCoordinatorRuntimeParams);

    // Verify and Assert
    final ArgumentCaptor<ServiceEventBuilder> emittedEventCaptor = ArgumentCaptor.forClass(ServiceEventBuilder.class);
    Mockito.verify(mockServiceEmitter).emit(emittedEventCaptor.capture());
    Assert.assertEquals(KillCompactionConfig.COUNT_METRIC, emittedEventCaptor.getValue().build(ImmutableMap.of()).toMap().get("metric"));
    // Should delete 1 config
    Assert.assertEquals(1, emittedEventCaptor.getValue().build(ImmutableMap.of()).toMap().get("value"));

    // Should call watch (to refresh current compaction config) four times due to RetryableException when failed
    Mockito.verify(mockJacksonConfigManager, Mockito.times(4)).watch(
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.class),
        ArgumentMatchers.eq(CoordinatorCompactionConfig.empty())
    );
    // Should call set (to try set new updated compaction config) four times due to RetryableException when failed
    Mockito.verify(mockJacksonConfigManager, Mockito.times(4)).set(
        ArgumentMatchers.eq(CoordinatorCompactionConfig.CONFIG_KEY),
        ArgumentMatchers.any(CoordinatorCompactionConfig.class),
        ArgumentMatchers.any(CoordinatorCompactionConfig.class),
        ArgumentMatchers.any()
    );
    Mockito.verifyNoMoreInteractions(mockJacksonConfigManager);
    // Should call retrieveAllDataSourceNames four times due to RetryableException when failed
    Mockito.verify(mockSqlSegmentsMetadataManager, Mockito.times(4)).retrieveAllDataSourceNames();
    Mockito.verifyNoMoreInteractions(mockSqlSegmentsMetadataManager);
  }
}
