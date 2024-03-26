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
import org.apache.druid.error.DruidException;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.metadata.SortOrder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.metadata.AvailableSegmentMetadata;
import org.apache.druid.segment.metadata.CoordinatorSegmentMetadataCache;
import org.apache.druid.segment.metadata.DataSourceInformation;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentStatusInCluster;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MetadataResourceTest
{
  private static final String DATASOURCE1 = "datasource1";
  private static final String SEGMENT_START_INTERVAL = "2012-10-24";
  private static final int NUM_PARTITIONS = 2;
  private final DataSegment[] segments =
      CreateDataSegments.ofDatasource(DATASOURCE1)
          .startingAt(SEGMENT_START_INTERVAL)
          .forIntervals(3, Granularities.DAY)
          .withNumPartitions(NUM_PARTITIONS)
          .eachOfSizeInMb(500)
          .toArray(new DataSegment[0]);

  private final List<DataSegmentPlus> segmentsPlus = Arrays.stream(segments)
          .map(s -> new DataSegmentPlus(s, DateTimes.nowUtc(), DateTimes.nowUtc(), null))
          .collect(Collectors.toList());
  private HttpServletRequest request;
  private SegmentsMetadataManager segmentsMetadataManager;
  private IndexerMetadataStorageCoordinator storageCoordinator;
  private DruidCoordinator coordinator;


  private MetadataResource metadataResource;

  @Before
  public void setUp()
  {
    request = Mockito.mock(HttpServletRequest.class);
    Mockito.doReturn(Mockito.mock(AuthenticationResult.class))
           .when(request).getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT);

    segmentsMetadataManager = Mockito.mock(SegmentsMetadataManager.class);
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

    coordinator = Mockito.mock(DruidCoordinator.class);
    Mockito.doReturn(2).when(coordinator).getReplicationFactor(segments[0].getId());
    Mockito.doReturn(null).when(coordinator).getReplicationFactor(segments[1].getId());
    Mockito.doReturn(1).when(coordinator).getReplicationFactor(segments[2].getId());
    Mockito.doReturn(1).when(coordinator).getReplicationFactor(segments[3].getId());
    Mockito.doReturn(ImmutableSet.of(segments[3]))
           .when(dataSourcesSnapshot).getOvershadowedSegments();

    storageCoordinator = Mockito.mock(IndexerMetadataStorageCoordinator.class);
    Mockito.doReturn(segments[4])
           .when(storageCoordinator)
           .retrieveSegmentForId(segments[4].getId().toString(), false);
    Mockito.doReturn(null)
           .when(storageCoordinator)
           .retrieveSegmentForId(segments[5].getId().toString(), false);
    Mockito.doReturn(segments[5])
           .when(storageCoordinator)
           .retrieveSegmentForId(segments[5].getId().toString(), true);

    Mockito.doAnswer(mockIterateAllUnusedSegmentsForDatasource())
           .when(segmentsMetadataManager)
           .iterateAllUnusedSegmentsForDatasource(
               ArgumentMatchers.any(),
               ArgumentMatchers.any(),
               ArgumentMatchers.any(),
               ArgumentMatchers.any(),
               ArgumentMatchers.any());

    metadataResource = new MetadataResource(
        segmentsMetadataManager,
        storageCoordinator,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        coordinator,
        null
    );
  }

  @Test
  public void testGetAllSegmentsWithOvershadowedStatus()
  {
    Response response = metadataResource.getAllUsedSegments(request, null, "includeOvershadowedStatus", null);

    final List<SegmentStatusInCluster> resultList = extractResponseList(response);
    Assert.assertEquals(resultList.size(), 4);
    Assert.assertEquals(new SegmentStatusInCluster(segments[0], false, 2, null, false), resultList.get(0));
    Assert.assertEquals(new SegmentStatusInCluster(segments[1], false, null, null, false), resultList.get(1));
    Assert.assertEquals(new SegmentStatusInCluster(segments[2], false, 1, null, false), resultList.get(2));
    // Replication factor should be 0 as the segment is overshadowed
    Assert.assertEquals(new SegmentStatusInCluster(segments[3], true, 0, null, false), resultList.get(3));
  }

  @Test
  public void testGetAllSegmentsIncludingRealtime()
  {
    CoordinatorSegmentMetadataCache coordinatorSegmentMetadataCache = Mockito.mock(CoordinatorSegmentMetadataCache.class);

    String dataSource2 = "datasource2";

    DataSegment[] realTimeSegments =
        CreateDataSegments.ofDatasource(dataSource2)
                          .forIntervals(3, Granularities.DAY)
                          .withNumPartitions(2)
                          .eachOfSizeInMb(500)
                          .toArray(new DataSegment[0]);

    Mockito.doReturn(null).when(coordinator).getReplicationFactor(realTimeSegments[0].getId());
    Mockito.doReturn(null).when(coordinator).getReplicationFactor(realTimeSegments[1].getId());
    Map<SegmentId, AvailableSegmentMetadata> availableSegments = new HashMap<>();
    availableSegments.put(
        segments[0].getId(),
        AvailableSegmentMetadata.builder(
            segments[0],
            0L,
            Collections.emptySet(),
            null,
            20L
        ).build()
    );
    availableSegments.put(
        segments[1].getId(),
        AvailableSegmentMetadata.builder(
            segments[1],
            0L,
            Collections.emptySet(),
            null,
            30L
        ).build()
    );
    availableSegments.put(
        segments[1].getId(),
        AvailableSegmentMetadata.builder(
            segments[1],
            0L,
            Collections.emptySet(),
            null,
            30L
        ).build()
    );
    availableSegments.put(
        realTimeSegments[0].getId(),
        AvailableSegmentMetadata.builder(
            realTimeSegments[0],
            1L,
            Collections.emptySet(),
            null,
            10L
        ).build()
    );
    availableSegments.put(
        realTimeSegments[1].getId(),
        AvailableSegmentMetadata.builder(
            realTimeSegments[1],
            1L,
            Collections.emptySet(),
            null,
            40L
        ).build()
    );

    Mockito.doReturn(availableSegments).when(coordinatorSegmentMetadataCache).getSegmentMetadataSnapshot();

    Mockito.doReturn(availableSegments.get(segments[0].getId()))
           .when(coordinatorSegmentMetadataCache)
           .getAvailableSegmentMetadata(DATASOURCE1, segments[0].getId());

    Mockito.doReturn(availableSegments.get(segments[1].getId()))
           .when(coordinatorSegmentMetadataCache)
           .getAvailableSegmentMetadata(DATASOURCE1, segments[1].getId());

    metadataResource = new MetadataResource(
        segmentsMetadataManager,
        storageCoordinator,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        coordinator,
        coordinatorSegmentMetadataCache
    );

    Response response = metadataResource.getAllUsedSegments(request, null, "includeOvershadowedStatus", "includeRealtimeSegments");

    final List<SegmentStatusInCluster> resultList = extractResponseList(response);
    Assert.assertEquals(resultList.size(), 6);
    Assert.assertEquals(new SegmentStatusInCluster(segments[0], false, 2, 20L, false), resultList.get(0));
    Assert.assertEquals(new SegmentStatusInCluster(segments[1], false, null, 30L, false), resultList.get(1));
    Assert.assertEquals(new SegmentStatusInCluster(segments[2], false, 1, null, false), resultList.get(2));
    // Replication factor should be 0 as the segment is overshadowed
    Assert.assertEquals(new SegmentStatusInCluster(segments[3], true, 0, null, false), resultList.get(3));
    Assert.assertEquals(new SegmentStatusInCluster(realTimeSegments[0], false, null, 10L, true), resultList.get(4));
    Assert.assertEquals(new SegmentStatusInCluster(realTimeSegments[1], false, null, 40L, true), resultList.get(5));
  }


  @Test
  public void testGetUnusedSegmentsInDataSourceWithValidDataSource()
  {
    final Response response = metadataResource
        .getUnusedSegmentsInDataSource(request, DATASOURCE1, null, null, null, null);
    final List<DataSegmentPlus> observedSegments = extractResponseList(response);
    Assert.assertEquals(segmentsPlus, observedSegments);
  }

  @Test
  public void testGetUnusedSegmentsInDataSourceWithIntervalFilter()
  {
    final String interval = SEGMENT_START_INTERVAL + "_P2D";
    final int expectedLimit = NUM_PARTITIONS * 2;

    final Response response = metadataResource
        .getUnusedSegmentsInDataSource(request, DATASOURCE1, interval, null, null, null);
    final List<DataSegmentPlus> observedSegments = extractResponseList(response);
    Assert.assertEquals(expectedLimit, observedSegments.size());
    Assert.assertEquals(segmentsPlus.stream().limit(expectedLimit).collect(Collectors.toList()), observedSegments);
  }

  @Test
  public void testGetUnusedSegmentsInDataSourceWithLimitFilter()
  {
    final int limit = 3;
    final String interval = SEGMENT_START_INTERVAL + "_P2D";

    final Response response = metadataResource
        .getUnusedSegmentsInDataSource(request, DATASOURCE1, interval, limit, null, null);
    final List<DataSegmentPlus> observedSegments = extractResponseList(response);
    Assert.assertEquals(limit, observedSegments.size());
    Assert.assertEquals(segmentsPlus.stream().limit(limit).collect(Collectors.toList()), observedSegments);
  }

  @Test
  public void testGetUnusedSegmentsInDataSourceWithLimitAndLastSegmentIdFilter()
  {
    final int limit = 3;
    final String interval = SEGMENT_START_INTERVAL + "_P2D";
    final String lastSegmentId = segments[limit - 1].getId().toString();
    final Response response = metadataResource
        .getUnusedSegmentsInDataSource(request, DATASOURCE1, interval, limit, lastSegmentId, null);
    final List<DataSegmentPlus> observedSegments = extractResponseList(response);
    Assert.assertEquals(Collections.singletonList(segmentsPlus.get(limit)), observedSegments);
  }

  @Test
  public void testGetUnusedSegmentsInDataSourceWithNonExistentDataSource()
  {
    final Response response = metadataResource
        .getUnusedSegmentsInDataSource(request, "non-existent", null, null, null, null);
    final List<DataSegmentPlus> observedSegments = extractResponseList(response);
    Assert.assertTrue(observedSegments.isEmpty());
  }

  @Test
  public void testGetUnusedSegmentsWithNoDataSource()
  {
    final Response response = metadataResource
        .getUnusedSegmentsInDataSource(request, null, null, null, null, null);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals("dataSourceName must be non-empty.", getExceptionMessage(response));
  }

  @Test
  public void testGetUnusedSegmentsWithEmptyDatasourceName()
  {
    final Response response = metadataResource
        .getUnusedSegmentsInDataSource(request, "", null, null, null, null);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals("dataSourceName must be non-empty.", getExceptionMessage(response));
  }

  @Test
  public void testGetUnusedSegmentsWithInvalidLimit()
  {
    final Response response = metadataResource
        .getUnusedSegmentsInDataSource(request, DATASOURCE1, null, -1, null, null);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals("Invalid limit[-1] specified. Limit must be > 0.", getExceptionMessage(response));
  }

  @Test
  public void testGetUnusedSegmentsWithInvalidLastSegmentId()
  {
    final Response response = metadataResource
        .getUnusedSegmentsInDataSource(request, DATASOURCE1, null, null, "invalid", null);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals("Invalid lastSegmentId[invalid] specified.", getExceptionMessage(response));
  }

  @Test
  public void testGetUnusedSegmentsWithInvalidInterval()
  {
    final Response response = metadataResource
        .getUnusedSegmentsInDataSource(request, DATASOURCE1, "2015/2014", null, null, null);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(
        "Bad interval[2015/2014]: [The end instant must be greater than the start instant]",
        getExceptionMessage(response)
    );
  }

  @Test
  public void testGetUnusedSegmentsWithInvalidSortOrder()
  {
    final Response response = metadataResource
        .getUnusedSegmentsInDataSource(request, DATASOURCE1, null, null, null, "Ascd");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(
        "Unexpected value[Ascd] for SortOrder. Possible values are: [ASC, DESC]",
        getExceptionMessage(response)
    );
  }

  private Answer<Iterable<DataSegmentPlus>> mockIterateAllUnusedSegmentsForDatasource()
  {
    return invocationOnMock -> {
      String dataSourceName = invocationOnMock.getArgument(0);
      Interval interval = invocationOnMock.getArgument(1);
      Integer limit = invocationOnMock.getArgument(2);
      String lastSegmentId = invocationOnMock.getArgument(3);
      SortOrder sortOrder = invocationOnMock.getArgument(4);
      if (!DATASOURCE1.equals(dataSourceName)) {
        return ImmutableList.of();
      }

      return segmentsPlus.stream()
          .filter(d -> d.getDataSegment().getDataSource().equals(dataSourceName)
                       && (interval == null
                           || (d.getDataSegment().getInterval().getStartMillis() >= interval.getStartMillis()
                               && d.getDataSegment().getInterval().getEndMillis() <= interval.getEndMillis()))
                       && (lastSegmentId == null
                           || (sortOrder == null && d.getDataSegment().getId().toString().compareTo(lastSegmentId) > 0)
                           || (sortOrder == SortOrder.ASC && d.getDataSegment().getId().toString().compareTo(lastSegmentId) > 0)
                           || (sortOrder == SortOrder.DESC && d.getDataSegment().getId().toString().compareTo(lastSegmentId) < 0)))
          .sorted((o1, o2) -> Comparators.intervalsByStartThenEnd()
              .compare(o1.getDataSegment().getInterval(), o2.getDataSegment().getInterval()))
          .limit(limit != null
              ? limit
              : segments.length)
          .collect(Collectors.toList());
    };
  }

  private static String getExceptionMessage(final Response response)
  {
    if (response.getEntity() instanceof DruidException) {
      return ((DruidException) response.getEntity()).getMessage();
    } else {
      return ((ErrorResponse) response.getEntity()).getUnderlyingException().getMessage();
    }
  }

  @Test
  public void testGetDataSourceInformation()
  {
    CoordinatorSegmentMetadataCache coordinatorSegmentMetadataCache = Mockito.mock(CoordinatorSegmentMetadataCache.class);
    Map<String, DataSourceInformation> dataSourceInformationMap = new HashMap<>();

    dataSourceInformationMap.put(
        DATASOURCE1,
        new DataSourceInformation(
            DATASOURCE1,
            RowSignature.builder()
                        .add("c1", ColumnType.FLOAT)
                         .add("c2", ColumnType.DOUBLE)
                        .build()
        )
    );

    dataSourceInformationMap.put(
        "datasource2",
        new DataSourceInformation(
            "datasource2",
            RowSignature.builder()
                        .add("d1", ColumnType.FLOAT)
                        .add("d2", ColumnType.DOUBLE)
                        .build()
        )
    );

    Mockito.doReturn(dataSourceInformationMap).when(coordinatorSegmentMetadataCache).getDataSourceInformationMap();

    metadataResource = new MetadataResource(
        segmentsMetadataManager,
        storageCoordinator,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        coordinator,
        coordinatorSegmentMetadataCache
    );

    Response response = metadataResource.getDataSourceInformation(request, Collections.singletonList(DATASOURCE1));

    List<DataSourceInformation> dataSourceInformations = extractResponseList(response);
    Assert.assertEquals(dataSourceInformations.size(), 1);
    Assert.assertEquals(dataSourceInformations.get(0), dataSourceInformationMap.get(DATASOURCE1));
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

  private <T> List<T> extractResponseList(Response response)
  {
    return Lists.newArrayList(
        (Iterable<T>) response.getEntity()
    );
  }
}
