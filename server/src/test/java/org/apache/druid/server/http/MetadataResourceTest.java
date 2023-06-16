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

package org.apache.druid.server.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentStatusInCluster;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class MetadataResourceTest
{
  private static final String DATASOURCE1 = "datasource1";
  private static final String DATASOURCE2 = "datasource2";

  private MetadataResource metadataResource;

  private SegmentsMetadataManager segmentsMetadataManager;
  private DruidCoordinator coordinator;
  private HttpServletRequest request;

  private final DataSegment dataSegment1 = new DataSegment(
      DATASOURCE1,
      Intervals.of("2010-01-01/P1D"),
      "v0",
      null,
      null,
      null,
      null,
      0x9,
      10
  );

  private final DataSegment dataSegment2 = new DataSegment(
      DATASOURCE1,
      Intervals.of("2010-01-22/P1D"),
      "v0",
      null,
      null,
      null,
      null,
      0x9,
      20
  );

  private final DataSegment dataSegment3 = new DataSegment(
      DATASOURCE2,
      Intervals.of("2010-01-01/P1M"),
      "v0",
      null,
      null,
      null,
      null,
      0x9,
      30
  );

  private final DataSegment dataSegment4 = new DataSegment(
      DATASOURCE2,
      Intervals.of("2010-01-02/P1D"),
      "v0",
      null,
      null,
      null,
      null,
      0x9,
      35
  );

  @Before
  public void setUp() throws Exception
  {
    // Create mock request
    request = mock(HttpServletRequest.class);
    doReturn(mock(AuthenticationResult.class)).when(request).getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT);

    // Mock SegmentsMetadataManager
    segmentsMetadataManager = mock(SegmentsMetadataManager.class);
    ImmutableDruidDataSource druidDataSource1 = new ImmutableDruidDataSource(
        DATASOURCE1,
        ImmutableMap.of(),
        ImmutableList.of(
            dataSegment1,
            dataSegment2
        )
    );

    ImmutableDruidDataSource druidDataSource2 = new ImmutableDruidDataSource(
        DATASOURCE1,
        ImmutableMap.of(),
        ImmutableList.of(
            dataSegment3,
            dataSegment4
        )
    );

    // Mock segments from cache and coordinator
    DataSourcesSnapshot dataSourcesSnapshot = mock(DataSourcesSnapshot.class);
    doReturn(dataSourcesSnapshot).when(segmentsMetadataManager).getSnapshotOfDataSourcesWithAllUsedSegments();

    doReturn(ImmutableList.of(druidDataSource1, druidDataSource2)).when(dataSourcesSnapshot).getDataSourcesWithAllUsedSegments();
    // Segment 4 is overshadowed

    // Mock Coordinator
    coordinator = mock(DruidCoordinator.class);
    // Segment 1: Replication factor 2, not overshadowed
    doReturn(2).when(coordinator).getReplicationFactorForSegment(dataSegment1.getId());
    // Segment 2: Replication factor null, not overshadowed
    doReturn(null).when(coordinator).getReplicationFactorForSegment(dataSegment2.getId());
    // Segment 3: Replication factor 1, not overshadowed
    doReturn(1).when(coordinator).getReplicationFactorForSegment(dataSegment3.getId());
    // Segment 4: Replication factor 1, overshadowed
    doReturn(1).when(coordinator).getReplicationFactorForSegment(dataSegment4.getId());
    doReturn(ImmutableSet.of(dataSegment4)).when(dataSourcesSnapshot).getOvershadowedSegments();

    metadataResource = new MetadataResource(segmentsMetadataManager, mock(IndexerMetadataStorageCoordinator.class), AuthTestUtils.TEST_AUTHORIZER_MAPPER, coordinator, new ObjectMapper());
  }

  @Test
  public void testGetAllSegmentsWithOvershadowedStatus()
  {
    Response response = metadataResource.getAllUsedSegments(
        request,
        null,
        "includeOvershadowedStatus"
    );

    List<SegmentStatusInCluster> resultList = materializeResponse(response);
    Assert.assertEquals(resultList.size(), 4);
    Assert.assertEquals(new SegmentStatusInCluster(dataSegment1, false, 2), resultList.get(0));
    Assert.assertEquals(new SegmentStatusInCluster(dataSegment2, false, null), resultList.get(1));
    Assert.assertEquals(new SegmentStatusInCluster(dataSegment3, false, 1), resultList.get(2));
    // Replication factor should be 0 as the segment is overshadowed
    Assert.assertEquals(new SegmentStatusInCluster(dataSegment4, true, 0), resultList.get(3));
  }

  private List<SegmentStatusInCluster> materializeResponse(Response response)
  {
    Iterable<SegmentStatusInCluster> resultIterator = (Iterable<SegmentStatusInCluster>) response.getEntity();
    List<SegmentStatusInCluster> segmentStatusInClusters = new ArrayList<>();
    resultIterator.forEach(segmentStatusInClusters::add);
    return segmentStatusInClusters;
  }
}
