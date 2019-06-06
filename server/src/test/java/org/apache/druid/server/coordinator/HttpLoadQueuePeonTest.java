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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.server.ServerTestHelper;
import org.apache.druid.server.coordination.DataSegmentChangeRequest;
import org.apache.druid.server.coordination.SegmentLoadDropHandler;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.joda.time.Duration;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

/**
 */
public class HttpLoadQueuePeonTest
{
  final DataSegment segment1 = new DataSegment(
      "test1", Intervals.of("2014/2015"), "v1",
      null, null, null, null, 0, 0
  );

  final DataSegment segment2 = new DataSegment(
      "test2", Intervals.of("2014/2015"), "v1",
      null, null, null, null, 0, 0
  );

  final DataSegment segment3 = new DataSegment(
      "test3", Intervals.of("2014/2015"), "v1",
      null, null, null, null, 0, 0
  );

  final DataSegment segment4 = new DataSegment(
      "test4", Intervals.of("2014/2015"), "v1",
      null, null, null, null, 0, 0
  );

  final TestDruidCoordinatorConfig config = new TestDruidCoordinatorConfig(
      null,
      null,
      null,
      null,
      null,
      null,
      10,
      null,
      Duration.ZERO
  )
  {
    @Override
    public int getHttpLoadQueuePeonBatchSize()
    {
      return 2;
    }
  };

  @Test(timeout = 60_000L)
  public void testSimple() throws Exception
  {
    HttpLoadQueuePeon httpLoadQueuePeon = new HttpLoadQueuePeon(
        "http://dummy:4000",
        ServerTestHelper.MAPPER,
        new TestHttpClient(),
        config,
        Executors.newScheduledThreadPool(
            2,
            Execs.makeThreadFactory("HttpLoadQueuePeonTest-%s")
        ),
        Execs.singleThreaded("HttpLoadQueuePeonTest")
    );

    httpLoadQueuePeon.start();

    Map<SegmentId, CountDownLatch> latches = ImmutableMap.of(
        segment1.getId(), new CountDownLatch(1),
        segment2.getId(), new CountDownLatch(1),
        segment3.getId(), new CountDownLatch(1),
        segment4.getId(), new CountDownLatch(1)
    );

    httpLoadQueuePeon.dropSegment(segment1, () -> latches.get(segment1.getId()).countDown());
    httpLoadQueuePeon.loadSegment(segment2, () -> latches.get(segment2.getId()).countDown());
    httpLoadQueuePeon.dropSegment(segment3, () -> latches.get(segment3.getId()).countDown());
    httpLoadQueuePeon.loadSegment(segment4, () -> latches.get(segment4.getId()).countDown());

    latches.get(segment1.getId()).await();
    latches.get(segment2.getId()).await();
    latches.get(segment3.getId()).await();
    latches.get(segment4.getId()).await();

    httpLoadQueuePeon.stop();
  }

  @Test(timeout = 60_000L)
  public void testLoadDropAfterStop() throws Exception
  {
    HttpLoadQueuePeon httpLoadQueuePeon = new HttpLoadQueuePeon(
        "http://dummy:4000",
        ServerTestHelper.MAPPER,
        new TestHttpClient(),
        config,
        Executors.newScheduledThreadPool(
            2,
            Execs.makeThreadFactory("HttpLoadQueuePeonTest-%s")
        ),
        Execs.singleThreaded("HttpLoadQueuePeonTest")
    );

    httpLoadQueuePeon.start();

    Map<SegmentId, CountDownLatch> latches = ImmutableMap.of(
        segment1.getId(), new CountDownLatch(1),
        segment2.getId(), new CountDownLatch(1),
        segment3.getId(), new CountDownLatch(1),
        segment4.getId(), new CountDownLatch(1)
    );

    httpLoadQueuePeon.dropSegment(segment1, () -> latches.get(segment1.getId()).countDown());
    httpLoadQueuePeon.loadSegment(segment2, () -> latches.get(segment2.getId()).countDown());
    latches.get(segment1.getId()).await();
    latches.get(segment2.getId()).await();
    httpLoadQueuePeon.stop();
    httpLoadQueuePeon.dropSegment(segment3, () -> latches.get(segment3.getId()).countDown());
    httpLoadQueuePeon.loadSegment(segment4, () -> latches.get(segment4.getId()).countDown());
    latches.get(segment3.getId()).await();
    latches.get(segment4.getId()).await();

  }

  private static class TestHttpClient implements HttpClient
  {
    @Override
    public <Intermediate, Final> ListenableFuture<Final> go(
        Request request,
        HttpResponseHandler<Intermediate, Final> httpResponseHandler
    )
    {
      throw new UnsupportedOperationException("Not Implemented.");
    }

    @Override
    public <Intermediate, Final> ListenableFuture<Final> go(
        Request request,
        HttpResponseHandler<Intermediate, Final> httpResponseHandler,
        Duration duration
    )
    {
      HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
      httpResponse.setContent(ChannelBuffers.buffer(0));
      httpResponseHandler.handleResponse(httpResponse, null);
      try {
        List<DataSegmentChangeRequest> changeRequests = ServerTestHelper.MAPPER.readValue(
            request.getContent().array(), new TypeReference<List<DataSegmentChangeRequest>>()
            {
            }
        );

        List<SegmentLoadDropHandler.DataSegmentChangeRequestAndStatus> statuses = new ArrayList<>(changeRequests.size());
        for (DataSegmentChangeRequest cr : changeRequests) {
          statuses.add(new SegmentLoadDropHandler.DataSegmentChangeRequestAndStatus(
              cr,
              SegmentLoadDropHandler.Status.SUCCESS
          ));
        }
        return (ListenableFuture) Futures.immediateFuture(
            new ByteArrayInputStream(
                ServerTestHelper.MAPPER
                    .writerWithType(HttpLoadQueuePeon.RESPONSE_ENTITY_TYPE_REF)
                    .writeValueAsBytes(statuses)
            )
        );
      }
      catch (Exception ex) {
        throw new RE(ex, "Unexpected exception.");
      }
    }
  }
}
