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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordination.BatchDataSegmentAnnouncer;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.server.coordination.DataSegmentChangeRequest;
import org.apache.druid.server.coordination.SegmentChangeRequestLoad;
import org.apache.druid.server.coordination.SegmentLoadDropHandler;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.mocks.MockAsyncContext;
import org.apache.druid.server.mocks.MockHttpServletRequest;
import org.apache.druid.server.mocks.MockHttpServletResponse;
import org.apache.druid.timeline.DataSegment;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class SegmentListerResourceTest
{
  private static final List<DataSegment> SEGMENTS = CreateDataSegments
      .ofDatasource(TestDataSource.WIKI)
      .withNumPartitions(10)
      .eachOfSizeInMb(100);

  private SegmentListerResource segmentListerResource;
  private BatchDataSegmentAnnouncer segmentAnnouncer;

  @Before
  public void setup()
  {
    final SegmentLoadDropHandler loadDropHandler = Mockito.mock(SegmentLoadDropHandler.class);
    this.segmentAnnouncer = Mockito.mock(BatchDataSegmentAnnouncer.class);
    this.segmentListerResource = new SegmentListerResource(
        TestHelper.JSON_MAPPER,
        TestHelper.makeSmileMapper(),
        segmentAnnouncer,
        loadDropHandler
    );
  }

  @Test
  public void test_getSegments_returnsAllSegments_onFirstRequest() throws Exception
  {
    Mockito.when(
        segmentAnnouncer.getSegmentChangesSince(new ChangeRequestHistory.Counter(0, 0))
    ).thenReturn(
        Futures.immediateFuture(
            ChangeRequestsSnapshot.success(
                new ChangeRequestHistory.Counter(0, 0),
                SEGMENTS.stream().map(SegmentChangeRequestLoad::new).collect(Collectors.toList())
            )
        )
    );

    final MockHttpServletResponse response = new MockHttpServletResponse();
    final HttpServletRequest request = createMockRequest(response);

    segmentListerResource.getSegments(0, 0, 10L, request);

    Assertions.assertEquals(response.getStatus(), 200);

    final ChangeRequestsSnapshot<DataSegmentChangeRequest> responsePayload = getResponsePayload(response);
    Assertions.assertEquals(
        responsePayload.getCounter(),
        new ChangeRequestHistory.Counter(0, 0)
    );
    Assertions.assertNotNull(responsePayload.getRequests());
    Assertions.assertEquals(
        SEGMENTS.size(),
        responsePayload.getRequests().size()
    );
  }

  private ChangeRequestsSnapshot<DataSegmentChangeRequest> getResponsePayload(
      MockHttpServletResponse response
  ) throws IOException
  {
    return TestHelper.JSON_MAPPER.readValue(
        response.baos.toByteArray(),
        new TypeReference<>() {}
    );
  }

  private HttpServletRequest createMockRequest(MockHttpServletResponse response)
  {
    final MockHttpServletRequest request = new MockHttpServletRequest();

    final MockAsyncContext asyncContext = new MockAsyncContext();
    asyncContext.request = request;
    asyncContext.response = response;

    request.asyncContextSupplier = () -> asyncContext;
    return request;
  }
}
