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

import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.metadata.TestSupervisorSpec;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.TestDruidCoordinatorConfig;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KillDatasourceMetadataTest
{
  @Mock
  private IndexerMetadataStorageCoordinator mockIndexerMetadataStorageCoordinator;

  @Mock
  private MetadataSupervisorManager mockMetadataSupervisorManager;

  @Mock
  private DruidCoordinatorRuntimeParams mockDruidCoordinatorRuntimeParams;

  @Mock
  private TestSupervisorSpec mockKinesisSupervisorSpec;

  @Mock
  private ServiceEmitter mockServiceEmitter;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private KillDatasourceMetadata killDatasourceMetadata;

  @Test
  public void testRunSkipIfLastRunLessThanPeriod()
  {
    TestDruidCoordinatorConfig druidCoordinatorConfig = new TestDruidCoordinatorConfig.Builder()
        .withMetadataStoreManagementPeriod(new Duration("PT5S"))
        .withCoordinatorDatasourceKillPeriod(new Duration(Long.MAX_VALUE))
        .withCoordinatorDatasourceKillDurationToRetain(new Duration("PT1S"))
        .withCoordinatorKillMaxSegments(10)
        .withCoordinatorKillIgnoreDurationToRetain(false)
        .build();
    killDatasourceMetadata = new KillDatasourceMetadata(druidCoordinatorConfig, mockIndexerMetadataStorageCoordinator, mockMetadataSupervisorManager);
    killDatasourceMetadata.run(mockDruidCoordinatorRuntimeParams);
    Mockito.verifyNoInteractions(mockIndexerMetadataStorageCoordinator);
    Mockito.verifyNoInteractions(mockMetadataSupervisorManager);
  }

  @Test
  public void testRunNotSkipIfLastRunMoreThanPeriod()
  {
    Mockito.when(mockDruidCoordinatorRuntimeParams.getEmitter()).thenReturn(mockServiceEmitter);

    TestDruidCoordinatorConfig druidCoordinatorConfig = new TestDruidCoordinatorConfig.Builder()
        .withMetadataStoreManagementPeriod(new Duration("PT5S"))
        .withCoordinatorDatasourceKillPeriod(new Duration("PT6S"))
        .withCoordinatorDatasourceKillDurationToRetain(new Duration("PT1S"))
        .withCoordinatorKillMaxSegments(10)
        .withCoordinatorKillIgnoreDurationToRetain(false)
        .build();
    killDatasourceMetadata = new KillDatasourceMetadata(druidCoordinatorConfig, mockIndexerMetadataStorageCoordinator, mockMetadataSupervisorManager);
    killDatasourceMetadata.run(mockDruidCoordinatorRuntimeParams);
    Mockito.verify(mockIndexerMetadataStorageCoordinator).removeDataSourceMetadataOlderThan(ArgumentMatchers.anyLong(), ArgumentMatchers.anySet());
    Mockito.verify(mockServiceEmitter).emit(ArgumentMatchers.any(ServiceEventBuilder.class));
  }

  @Test
  public void testConstructorFailIfInvalidPeriod()
  {
    TestDruidCoordinatorConfig druidCoordinatorConfig = new TestDruidCoordinatorConfig.Builder()
        .withMetadataStoreManagementPeriod(new Duration("PT5S"))
        .withCoordinatorDatasourceKillPeriod(new Duration("PT3S"))
        .withCoordinatorDatasourceKillDurationToRetain(new Duration("PT1S"))
        .withCoordinatorKillMaxSegments(10)
        .withCoordinatorKillIgnoreDurationToRetain(false)
        .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Coordinator datasource metadata kill period must be >= druid.coordinator.period.metadataStoreManagementPeriod");
    killDatasourceMetadata = new KillDatasourceMetadata(druidCoordinatorConfig, mockIndexerMetadataStorageCoordinator, mockMetadataSupervisorManager);
  }

  @Test
  public void testConstructorFailIfInvalidRetainDuration()
  {
    TestDruidCoordinatorConfig druidCoordinatorConfig = new TestDruidCoordinatorConfig.Builder()
        .withMetadataStoreManagementPeriod(new Duration("PT5S"))
        .withCoordinatorDatasourceKillPeriod(new Duration("PT6S"))
        .withCoordinatorDatasourceKillDurationToRetain(new Duration("PT-1S"))
        .withCoordinatorKillMaxSegments(10)
        .withCoordinatorKillIgnoreDurationToRetain(false)
        .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Coordinator datasource metadata kill retainDuration must be >= 0");
    killDatasourceMetadata = new KillDatasourceMetadata(druidCoordinatorConfig, mockIndexerMetadataStorageCoordinator, mockMetadataSupervisorManager);
  }

  @Test
  public void testRunWithEmptyFilterExcludedDatasource()
  {
    Mockito.when(mockDruidCoordinatorRuntimeParams.getEmitter()).thenReturn(mockServiceEmitter);

    TestDruidCoordinatorConfig druidCoordinatorConfig = new TestDruidCoordinatorConfig.Builder()
        .withMetadataStoreManagementPeriod(new Duration("PT5S"))
        .withCoordinatorDatasourceKillPeriod(new Duration("PT6S"))
        .withCoordinatorDatasourceKillDurationToRetain(new Duration("PT1S"))
        .withCoordinatorKillMaxSegments(10)
        .withCoordinatorKillIgnoreDurationToRetain(false)
        .build();
    killDatasourceMetadata = new KillDatasourceMetadata(druidCoordinatorConfig, mockIndexerMetadataStorageCoordinator, mockMetadataSupervisorManager);
    killDatasourceMetadata.run(mockDruidCoordinatorRuntimeParams);
    Mockito.verify(mockIndexerMetadataStorageCoordinator).removeDataSourceMetadataOlderThan(ArgumentMatchers.anyLong(), ArgumentMatchers.eq(ImmutableSet.of()));
    Mockito.verify(mockServiceEmitter).emit(ArgumentMatchers.any(ServiceEventBuilder.class));
  }
}
