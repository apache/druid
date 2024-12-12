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

package org.apache.druid.indexing.overlord.http;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.rpc.indexing.SegmentUpdateResponse;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.simulate.TestSegmentsMetadataManager;
import org.apache.druid.server.http.SegmentsToUpdateFilter;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class OverlordDataSourcesResourceTest
{
  private static final String WIKI_SEGMENTS_START = "2024-01-01";
  private static final List<DataSegment> WIKI_SEGMENTS_10X1D
      = CreateDataSegments.ofDatasource(TestDataSource.WIKI)
                          .forIntervals(10, Granularities.DAY)
                          .startingAt(WIKI_SEGMENTS_START)
                          .eachOfSizeInMb(500);

  private TestSegmentsMetadataManager segmentsMetadataManager;

  private OverlordDataSourcesResource dataSourcesResource;

  @Before
  public void setup()
  {
    AuditManager auditManager = EasyMock.createStrictMock(AuditManager.class);
    segmentsMetadataManager = new TestSegmentsMetadataManager();

    TaskMaster taskMaster = new TaskMaster(null, null);
    dataSourcesResource = new OverlordDataSourcesResource(
        taskMaster,
        segmentsMetadataManager,
        auditManager
    );
    taskMaster.becomeFullLeader();

    WIKI_SEGMENTS_10X1D.forEach(segmentsMetadataManager::addSegment);
  }

  @Test
  public void testMarkSegmentAsUnused()
  {
    Response response = dataSourcesResource.markSegmentAsUnused(
        TestDataSource.WIKI,
        WIKI_SEGMENTS_10X1D.get(0).getId().toString()
    );
    verifyNumSegmentsUpdated(1, response);
  }

  @Test
  public void testMarkSegmentAsUnused_withInvalidSegmentId()
  {
    Response response = dataSourcesResource.markSegmentAsUnused(
        TestDataSource.WIKI,
        "someSegment"
    );
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(
        "Could not parse Segment ID[someSegment] for DataSource[wiki]",
        response.getEntity()
    );
  }

  @Test
  public void testMarkAllSegmentsAsUnused()
  {
    Response response = dataSourcesResource.markAllSegmentsAsUnused(
        TestDataSource.WIKI,
        createHttpServletRequest()
    );
    verifyNumSegmentsUpdated(10, response);
  }

  @Test
  public void testMarkSegmentsAsUnused_bySegmentIds()
  {
    final Set<String> segmentIdsToUpdate = ImmutableSet.of(
        WIKI_SEGMENTS_10X1D.get(0).getId().toString(),
        WIKI_SEGMENTS_10X1D.get(8).getId().toString()
    );

    Response response = dataSourcesResource.markSegmentsAsUnused(
        TestDataSource.WIKI,
        new SegmentsToUpdateFilter(null, segmentIdsToUpdate, null),
        createHttpServletRequest()
    );
    verifyNumSegmentsUpdated(2, response);
  }

  @Test
  public void testMarkSegmentsAsUnused_byInterval()
  {
    final Interval nonOverlappingInterval = Intervals.of("1000/2000");
    Response response = dataSourcesResource.markSegmentsAsUnused(
        TestDataSource.WIKI,
        new SegmentsToUpdateFilter(nonOverlappingInterval, null, null),
        createHttpServletRequest()
    );
    verifyNumSegmentsUpdated(0, response);

    final Interval overlappingInterval3Days = Intervals.of(WIKI_SEGMENTS_START + "/P3D");
    response = dataSourcesResource.markSegmentsAsUnused(
        TestDataSource.WIKI,
        new SegmentsToUpdateFilter(overlappingInterval3Days, null, null),
        createHttpServletRequest()
    );
    verifyNumSegmentsUpdated(3, response);
  }

  @Test
  public void testMarkSegmentsAsUnused_byIntervalAndVersion()
  {
    final Interval overlappingInterval3Days = Intervals.of(WIKI_SEGMENTS_START + "/P3D");
    Response response = dataSourcesResource.markSegmentsAsUnused(
        TestDataSource.WIKI,
        new SegmentsToUpdateFilter(
            overlappingInterval3Days,
            null,
            Collections.singletonList("invalidVersion")
        ),
        createHttpServletRequest()
    );
    verifyNumSegmentsUpdated(0, response);

    final String wikiSegmentVersion = WIKI_SEGMENTS_10X1D.get(0).getVersion();
    response = dataSourcesResource.markSegmentsAsUnused(
        TestDataSource.WIKI,
        new SegmentsToUpdateFilter(
            overlappingInterval3Days,
            null,
            Collections.singletonList(wikiSegmentVersion)
        ),
        createHttpServletRequest()
    );
    verifyNumSegmentsUpdated(3, response);
  }

  @Test
  public void testMarkSegmentAsUsed()
  {
    final DataSegment segment = WIKI_SEGMENTS_10X1D.get(0);
    final String segmentId = segment.getId().toString();

    // Verify that segment which is already "used" is not updated
    Response response = dataSourcesResource.markSegmentAsUsed(TestDataSource.WIKI, segmentId);
    verifyNumSegmentsUpdated(0, response);

    // Mark segment as unused and then mark it as used
    dataSourcesResource.markSegmentAsUnused(TestDataSource.WIKI, segmentId);
    response = dataSourcesResource.markSegmentAsUsed(TestDataSource.WIKI, segmentId);
    verifyNumSegmentsUpdated(1, response);
  }

  @Test
  public void testMarkAllNonOvershadowedSegmentsAsUsed()
  {
    // Create higher version segments
    final String version2 = WIKI_SEGMENTS_10X1D.get(0).getVersion() + "__2";
    final List<DataSegment> wikiSegmentsV2 = WIKI_SEGMENTS_10X1D.stream().map(
        segment -> DataSegment.builder(segment).version(version2).build()
    ).collect(Collectors.toList());

    wikiSegmentsV2.forEach(segmentsMetadataManager::addSegment);

    // Mark all segments as unused
    Response response = dataSourcesResource.markAllSegmentsAsUnused(
        TestDataSource.WIKI,
        createHttpServletRequest()
    );
    verifyNumSegmentsUpdated(20, response);

    // Verify that only higher version segments are marked as used
    response = dataSourcesResource.markAllNonOvershadowedSegmentsAsUsed(
        TestDataSource.WIKI,
        createHttpServletRequest()
    );
    verifyNumSegmentsUpdated(10, response);

    final ImmutableDruidDataSource dataSource = segmentsMetadataManager
        .getImmutableDataSourceWithUsedSegments(TestDataSource.WIKI);
    Assert.assertNotNull(dataSource);

    final Collection<DataSegment> usedSegments = dataSource.getSegments();
    Assert.assertEquals(10, usedSegments.size());
    for (DataSegment segment : usedSegments) {
      Assert.assertEquals(version2, segment.getVersion());
    }
  }

  @Test
  public void testMarkNonOvershadowedSegmentsAsUsed_byInterval()
  {
    dataSourcesResource.markAllSegmentsAsUnused(TestDataSource.WIKI, createHttpServletRequest());

    final Interval disjointInterval = Intervals.of("1000/2000");
    Response response = dataSourcesResource.markNonOvershadowedSegmentsAsUsed(
        TestDataSource.WIKI,
        new SegmentsToUpdateFilter(disjointInterval, null, null)
    );
    verifyNumSegmentsUpdated(0, response);

    final Interval overlappingInterval3Days = Intervals.of(WIKI_SEGMENTS_START + "/P3D");
    response = dataSourcesResource.markNonOvershadowedSegmentsAsUsed(
        TestDataSource.WIKI,
        new SegmentsToUpdateFilter(overlappingInterval3Days, null, null)
    );
    verifyNumSegmentsUpdated(3, response);
  }

  @Test
  public void testMarkNonOvershadowedSegmentsAsUsed_byIntervalAndVersion()
  {
    dataSourcesResource.markAllSegmentsAsUnused(TestDataSource.WIKI, createHttpServletRequest());

    final Interval overlappingInterval4Days = Intervals.of(WIKI_SEGMENTS_START + "/P4D");
    Response response = dataSourcesResource.markNonOvershadowedSegmentsAsUsed(
        TestDataSource.WIKI,
        new SegmentsToUpdateFilter(
            overlappingInterval4Days,
            null,
            Collections.singletonList("invalidVersion")
        )
    );
    verifyNumSegmentsUpdated(0, response);

    final String wikiSegmentsVersion = WIKI_SEGMENTS_10X1D.get(0).getVersion();
    response = dataSourcesResource.markNonOvershadowedSegmentsAsUsed(
        TestDataSource.WIKI,
        new SegmentsToUpdateFilter(
            overlappingInterval4Days,
            null,
            Collections.singletonList(wikiSegmentsVersion)
        )
    );
    verifyNumSegmentsUpdated(4, response);
  }

  @Test
  public void testMarkNonOvershadowedSegmentsAsUsed_bySegmentIds()
  {
    dataSourcesResource.markAllSegmentsAsUnused(TestDataSource.WIKI, createHttpServletRequest());

    final Set<String> segmentIdsToUpdate = ImmutableSet.of(
        WIKI_SEGMENTS_10X1D.get(0).getId().toString(),
        WIKI_SEGMENTS_10X1D.get(1).getId().toString()
    );
    Response response = dataSourcesResource.markNonOvershadowedSegmentsAsUsed(
        TestDataSource.WIKI,
        new SegmentsToUpdateFilter(null, segmentIdsToUpdate, null)
    );
    verifyNumSegmentsUpdated(2, response);
  }

  @Test
  public void testMarkNonOvershadowedSegmentsAsUsed_withNullPayload_throwsBadRequestError()
  {
    final Response response
        = dataSourcesResource.markNonOvershadowedSegmentsAsUsed(TestDataSource.WIKI, null);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(
        "Invalid request payload. Specify either 'interval' or 'segmentIds', but not both."
        + " Optionally, include 'versions' only when 'interval' is provided.",
        response.getEntity()
    );
  }

  @Test
  public void testMarkNonOvershadowedSegmentsAsUsed_withInvalidPayload_throwsBadRequestError()
  {
    final String segmentId = WIKI_SEGMENTS_10X1D.get(0).getId().toString();
    final String expectedErrorMessage
        = "Invalid request payload. Specify either 'interval' or 'segmentIds', but not both."
          + " Optionally, include 'versions' only when 'interval' is provided.";

    // Both interval and segmentIds are null
    Response response = dataSourcesResource.markNonOvershadowedSegmentsAsUsed(
        TestDataSource.WIKI,
        new SegmentsToUpdateFilter(null, null, null)
    );
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(expectedErrorMessage, response.getEntity());

    // interval is null and segmentIds is empty
    response = dataSourcesResource.markNonOvershadowedSegmentsAsUsed(
        TestDataSource.WIKI,
        new SegmentsToUpdateFilter(null, Collections.emptySet(), null)
    );
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(expectedErrorMessage, response.getEntity());

    // Both interval and segmentIds are specified
    response = dataSourcesResource.markNonOvershadowedSegmentsAsUsed(
        TestDataSource.WIKI,
        new SegmentsToUpdateFilter(Intervals.of("1000/2000"), Collections.singleton(segmentId), null)
    );
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(expectedErrorMessage, response.getEntity());

    // versions are specified with segmentIds
    response = dataSourcesResource.markNonOvershadowedSegmentsAsUsed(
        TestDataSource.WIKI,
        new SegmentsToUpdateFilter(null, Collections.singleton(segmentId), Collections.singletonList("v1"))
    );
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(expectedErrorMessage, response.getEntity());
  }

  private void verifyNumSegmentsUpdated(int expectedUpdatedCount, Response response)
  {
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(new SegmentUpdateResponse(expectedUpdatedCount), response.getEntity());
  }

  private static HttpServletRequest createHttpServletRequest()
  {
    final HttpServletRequest request = EasyMock.createStrictMock(HttpServletRequest.class);

    EasyMock.expect(request.getHeader(AuditManager.X_DRUID_AUTHOR)).andReturn("author").anyTimes();
    EasyMock.expect(request.getHeader(AuditManager.X_DRUID_COMMENT)).andReturn("comment").anyTimes();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(null).anyTimes();
    EasyMock.expect(request.getRemoteAddr()).andReturn("127.0.0.1").anyTimes();

    EasyMock.expect(request.getMethod()).andReturn("POST").anyTimes();
    EasyMock.expect(request.getRequestURI()).andReturn("/request/uri").anyTimes();
    EasyMock.expect(request.getQueryString()).andReturn("query=string").anyTimes();

    EasyMock.replay(request);

    return request;
  }
}
