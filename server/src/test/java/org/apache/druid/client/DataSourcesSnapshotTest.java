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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.DataSegmentChange;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.utils.CircularBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class DataSourcesSnapshotTest
{

  @Test
  public void testGetChangesSince()
  {
    DataSegment dataSegment1 = dataSegmentWithIntervalAndVersion("2023-04-04T00:00:00Z/P1D", "v1");

    DataSourcesSnapshot dataSourcesSnapshotV1 = DataSourcesSnapshot.fromUsedSegments(
        Collections.singletonList(dataSegment1),
        ImmutableMap.of());

    CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChange>>> changesV1 = dataSourcesSnapshotV1.getChanges();
    Assert.assertEquals(0, changesV1.size());

    DataSourcesSnapshot dataSourcesSnapshotV2 = DataSourcesSnapshot.fromUsedSegments(
        Collections.singletonList(dataSegment1),
        ImmutableMap.of(),
        ImmutableMap.of(),
        DataSourcesSnapshot.getSegmentsWithOvershadowedStatus(
            dataSourcesSnapshotV1.getDataSourcesWithAllUsedSegments(),
            dataSourcesSnapshotV1.getOvershadowedSegments(),
            dataSourcesSnapshotV1.getHandedOffStatePerDataSource()),
        dataSourcesSnapshotV1.getChanges()
    );

    CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChange>>> changesV2 = dataSourcesSnapshotV2.getChanges();
    Assert.assertEquals(0, changesV2.size());

    DataSegment dataSegment2 = dataSegmentWithIntervalAndVersion("2023-04-05T01:00:00Z/P1D", "v1");
    DataSegment dataSegment3 = dataSegmentWithIntervalAndVersion("2023-04-06T01:00:00Z/P1D", "v1");
    DataSourcesSnapshot dataSourcesSnapshotV3 = DataSourcesSnapshot.fromUsedSegments(
        Lists.newArrayList(dataSegment2, dataSegment3),
        ImmutableMap.of(),
        ImmutableMap.of(),
        DataSourcesSnapshot.getSegmentsWithOvershadowedStatus(
            dataSourcesSnapshotV2.getDataSourcesWithAllUsedSegments(),
            dataSourcesSnapshotV2.getOvershadowedSegments(),
            dataSourcesSnapshotV2.getHandedOffStatePerDataSource()),
        dataSourcesSnapshotV2.getChanges()
    );

    CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChange>>> changesV3 = dataSourcesSnapshotV3.getChanges();
    ChangeRequestHistory.Counter counterV3 = DataSourcesSnapshot.getLastCounter(changesV3);
    Assert.assertEquals(1, changesV3.size());
    Assert.assertEquals(3, changesV3.get(0).getChangeRequest().size());
    Assert.assertEquals(1, counterV3.getCounter());

    DataSegment dataSegment4 = dataSegmentWithIntervalAndVersion("2023-04-07T01:00:00Z/P1D", "v1");
    DataSourcesSnapshot dataSourcesSnapshotV4 = DataSourcesSnapshot.fromUsedSegments(
        Lists.newArrayList(dataSegment3, dataSegment4),
        ImmutableMap.of(),
        ImmutableMap.of(),
        DataSourcesSnapshot.getSegmentsWithOvershadowedStatus(
            dataSourcesSnapshotV3.getDataSourcesWithAllUsedSegments(),
            dataSourcesSnapshotV3.getOvershadowedSegments(),
            dataSourcesSnapshotV3.getHandedOffStatePerDataSource()),
        dataSourcesSnapshotV3.getChanges()
    );

    CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChange>>> changesV4 = dataSourcesSnapshotV4.getChanges();
    ChangeRequestHistory.Counter counterV4 = DataSourcesSnapshot.getLastCounter(changesV4);
    Assert.assertEquals(2, changesV4.size());
    Assert.assertEquals(2, changesV4.get(1).getChangeRequest().size());
    Assert.assertEquals(2, counterV4.getCounter());

    DataSegment dataSegment5 = dataSegmentWithIntervalAndVersion("2023-04-08T01:00:00Z/P1D", "v1");
    DataSourcesSnapshot dataSourcesSnapshotV5 = DataSourcesSnapshot.fromUsedSegments(
        Lists.newArrayList(dataSegment3, dataSegment5),
        ImmutableMap.of(),
        ImmutableMap.of(dataSegment3.getDataSource(), new HashSet<>(Collections.singleton(dataSegment3.getId()))),
        DataSourcesSnapshot.getSegmentsWithOvershadowedStatus(
            dataSourcesSnapshotV4.getDataSourcesWithAllUsedSegments(),
            dataSourcesSnapshotV4.getOvershadowedSegments(),
            dataSourcesSnapshotV4.getHandedOffStatePerDataSource()),
        dataSourcesSnapshotV4.getChanges()
    );

    CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChange>>> changesV5 = dataSourcesSnapshotV5.getChanges();
    ChangeRequestHistory.Counter counterV5 = DataSourcesSnapshot.getLastCounter(changesV5);
    Assert.assertEquals(3, changesV5.size());
    Assert.assertEquals(3, changesV5.get(2).getChangeRequest().size());
    Assert.assertEquals(3, counterV5.getCounter());

    ChangeRequestHistory.Counter lastCounter = new ChangeRequestHistory.Counter(-1);
    ChangeRequestsSnapshot<DataSegmentChange> changeRequestsSnapshot = dataSourcesSnapshotV5.getChangesSince(lastCounter);
    Assert.assertTrue(changeRequestsSnapshot.isResetCounter());

    changeRequestsSnapshot = dataSourcesSnapshotV5.getChangesSince(ChangeRequestHistory.Counter.ZERO);
    Assert.assertFalse(changeRequestsSnapshot.isResetCounter());
    Assert.assertEquals(8, changeRequestsSnapshot.getRequests().size());

    changeRequestsSnapshot = dataSourcesSnapshotV5.getChangesSince(counterV3);
    Assert.assertFalse(changeRequestsSnapshot.isResetCounter());
    Assert.assertEquals(5, changeRequestsSnapshot.getRequests().size());

    changeRequestsSnapshot = dataSourcesSnapshotV5.getChangesSince(counterV4);
    Assert.assertFalse(changeRequestsSnapshot.isResetCounter());
    Assert.assertEquals(3, changeRequestsSnapshot.getRequests().size());

    changeRequestsSnapshot = dataSourcesSnapshotV5.getChangesSince(counterV5);
    Assert.assertFalse(changeRequestsSnapshot.isResetCounter());
    Assert.assertEquals(0, changeRequestsSnapshot.getRequests().size());

    changeRequestsSnapshot = dataSourcesSnapshotV5.getChangesSince(DataSourcesSnapshot.getLastCounter(dataSourcesSnapshotV5.getChanges()));
    Assert.assertFalse(changeRequestsSnapshot.isResetCounter());
    Assert.assertEquals(0, changeRequestsSnapshot.getRequests().size());
  }

  private DataSegment dataSegmentWithIntervalAndVersion(String intervalStr, String version)
  {
    return DataSegment.builder()
                      .dataSource("wikipedia")
                      .interval(Intervals.of(intervalStr))
                      .loadSpec(
                          ImmutableMap.of(
                              "type",
                              "local",
                              "path",
                              "somewhere"
                          )
                      )
                      .version(version)
                      .dimensions(ImmutableList.of())
                      .metrics(ImmutableList.of())
                      .shardSpec(NoneShardSpec.instance())
                      .binaryVersion(9)
                      .size(0)
                      .build();
  }
}
