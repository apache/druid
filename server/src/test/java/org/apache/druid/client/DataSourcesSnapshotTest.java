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

package org.apache.druid.client;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DataSourcesSnapshotTest
{
  private static final String DS1 = "ds1";
  private static final String DS2 = "ds2";
  private static final DateTime SNAPSHOT_TIME = DateTimes.of("2024-01-01");

  private static Set<DataSegment> segmentsOf(String dataSource, String start, int numIntervals)
  {
    return CreateDataSegments.ofDatasource(dataSource)
                             .forIntervals(numIntervals, Granularities.DAY)
                             .startingAt(start)
                             .withNumPartitions(1)
                             .eachOfSizeInMb(500)
                             .stream()
                             .collect(Collectors.toSet());
  }

  private static Set<String> idsOf(DataSourcesSnapshot snapshot, String dataSource)
  {
    return snapshot.getDataSource(dataSource).getSegments().stream()
                   .map(s -> s.getId().toString())
                   .collect(Collectors.toSet());
  }

  private static DataSourcesSnapshot baseSnapshot()
  {
    final Map<String, Set<DataSegment>> base = new HashMap<>();
    base.put(DS1, segmentsOf(DS1, "2024-01-01", 2));
    base.put(DS2, segmentsOf(DS2, "2024-01-01", 2));
    return DataSourcesSnapshot.fromUsedSegments(base, SNAPSHOT_TIME);
  }

  @Test
  public void testUpdateSnapshot_reusesUnchangedDatasourceByReference()
  {
    final DataSourcesSnapshot previous = baseSnapshot();

    final Set<DataSegment> newDs1 = segmentsOf(DS1, "2024-02-01", 3);
    final DataSourcesSnapshot updated = previous.updateSnapshotForDataSources(
        Map.of(DS1, newDs1),
        Set.of(),
        SNAPSHOT_TIME
    );

    Assert.assertSame(previous.getDataSource(DS2), updated.getDataSource(DS2));
    Assert.assertNotSame(previous.getDataSource(DS1), updated.getDataSource(DS1));
    Assert.assertEquals(
        newDs1.stream().map(s -> s.getId().toString()).collect(Collectors.toSet()),
        idsOf(updated, DS1)
    );
  }

  @Test
  public void testUpdateSnapshot_equalsFullRebuild()
  {
    final DataSourcesSnapshot previous = baseSnapshot();
    final Set<DataSegment> baseDs2 = previous.getDataSource(DS2).getSegments()
                                             .stream().collect(Collectors.toSet());

    final Set<DataSegment> newDs1 = segmentsOf(DS1, "2024-02-01", 3);
    final DataSourcesSnapshot incremental = previous.updateSnapshotForDataSources(
        Map.of(DS1, newDs1),
        Set.of(),
        SNAPSHOT_TIME
    );

    final Map<String, Set<DataSegment>> finalState = new HashMap<>();
    finalState.put(DS1, newDs1);
    finalState.put(DS2, baseDs2);
    final DataSourcesSnapshot fullRebuild = DataSourcesSnapshot.fromUsedSegments(finalState, SNAPSHOT_TIME);

    Assert.assertEquals(idsOf(fullRebuild, DS1), idsOf(incremental, DS1));
    Assert.assertEquals(idsOf(fullRebuild, DS2), idsOf(incremental, DS2));
    Assert.assertEquals(fullRebuild.getOvershadowedSegments(), incremental.getOvershadowedSegments());
  }

  @Test
  public void testUpdateSnapshot_removesDatasource()
  {
    final DataSourcesSnapshot previous = baseSnapshot();

    final DataSourcesSnapshot updated = previous.updateSnapshotForDataSources(
        Map.of(),
        Set.of(DS2),
        SNAPSHOT_TIME
    );

    Assert.assertNull(updated.getDataSource(DS2));
    Assert.assertNotNull(updated.getDataSource(DS1));
  }
}
