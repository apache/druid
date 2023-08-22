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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentStatusInCluster;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.List;

public class MetadataResourceTest
{
  private static final String DATASOURCE1 = "datasource1";

  private MetadataResource metadataResource;
  private HttpServletRequest request;

  private final DataSegment[] segments =
      CreateDataSegments.ofDatasource(DATASOURCE1)
                        .forIntervals(3, Granularities.DAY)
                        .withNumPartitions(2)
                        .eachOfSizeInMb(500)
                        .toArray(new DataSegment[0]);
  
  @Before
  public void setUp()
  {
    request = Mockito.mock(HttpServletRequest.class);
    Mockito.doReturn(Mockito.mock(AuthenticationResult.class))
           .when(request).getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT);

    SegmentsMetadataManager segmentsMetadataManager = Mockito.mock(SegmentsMetadataManager.class);
    ImmutableDruidDataSource druidDataSource1 = new ImmutableDruidDataSource(
        DATASOURCE1,
        ImmutableMap.of(),
        ImmutableList.of(segments[0], segments[1], segments[2], segments[3])
    );

    DataSourcesSnapshot dataSourcesSnapshot = Mockito.mock(DataSourcesSnapshot.class);
    Mockito.doReturn(dataSourcesSnapshot)
           .when(segmentsMetadataManager).getSnapshotOfDataSourcesWithAllUsedSegments();
    Mockito.doReturn(ImmutableList.of(druidDataSource1))
           .when(dataSourcesSnapshot).getDataSourcesWithAllUsedSegments();
    Mockito.doReturn(druidDataSource1)
           .when(segmentsMetadataManager)
           .getImmutableDataSourceWithUsedSegments(DATASOURCE1);

    DruidCoordinator coordinator = Mockito.mock(DruidCoordinator.class);
    Mockito.doReturn(2).when(coordinator).getReplicationFactor(segments[0].getId());
    Mockito.doReturn(null).when(coordinator).getReplicationFactor(segments[1].getId());
    Mockito.doReturn(1).when(coordinator).getReplicationFactor(segments[2].getId());
    Mockito.doReturn(1).when(coordinator).getReplicationFactor(segments[3].getId());
    Mockito.doReturn(ImmutableSet.of(segments[3]))
           .when(dataSourcesSnapshot).getOvershadowedSegments();

    IndexerMetadataStorageCoordinator storageCoordinator = Mockito.mock(IndexerMetadataStorageCoordinator.class);
    Mockito.doReturn(segments[4])
           .when(storageCoordinator)
           .retrieveSegmentForId(segments[4].getId().toString(), false);
    Mockito.doReturn(null)
           .when(storageCoordinator)
           .retrieveSegmentForId(segments[5].getId().toString(), false);
    Mockito.doReturn(segments[5])
           .when(storageCoordinator)
           .retrieveSegmentForId(segments[5].getId().toString(), true);

    metadataResource = new MetadataResource(
        segmentsMetadataManager,
        storageCoordinator,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        coordinator
    );
  }

  @Test
  public void testGetAllSegmentsWithOvershadowedStatus()
  {
    Response response = metadataResource.getAllUsedSegments(request, null, "includeOvershadowedStatus");

    final List<SegmentStatusInCluster> resultList = extractSegmentStatusList(response);
    Assert.assertEquals(resultList.size(), 4);
    Assert.assertEquals(new SegmentStatusInCluster(segments[0], false, 2), resultList.get(0));
    Assert.assertEquals(new SegmentStatusInCluster(segments[1], false, null), resultList.get(1));
    Assert.assertEquals(new SegmentStatusInCluster(segments[2], false, 1), resultList.get(2));
    // Replication factor should be 0 as the segment is overshadowed
    Assert.assertEquals(new SegmentStatusInCluster(segments[3], true, 0), resultList.get(3));
  }

  @Test
  public void testGetSegment()
  {
    // Available in snapshot
    Assert.assertEquals(
        segments[0],
        metadataResource.getSegment(segments[0].getDataSource(), segments[0].getId().toString(), null).getEntity()
    );

    // Unavailable in snapshot, but available in metadata
    Assert.assertEquals(
        segments[4],
        metadataResource.getSegment(segments[4].getDataSource(), segments[4].getId().toString(), null).getEntity()
    );

    // Unavailable and unused
    Assert.assertNull(
        metadataResource.getSegment(segments[5].getDataSource(), segments[5].getId().toString(), null).getEntity()
    );
    Assert.assertEquals(
        segments[5],
        metadataResource.getSegment(segments[5].getDataSource(), segments[5].getId().toString(), true).getEntity()
    );
  }

  private List<SegmentStatusInCluster> extractSegmentStatusList(Response response)
  {
    return Lists.newArrayList(
        (Iterable<SegmentStatusInCluster>) response.getEntity()
    );
  }
}
