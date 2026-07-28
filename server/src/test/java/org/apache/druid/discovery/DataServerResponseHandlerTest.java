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

package org.apache.druid.discovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.DefaultResponseContext;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;

import static org.apache.druid.query.Druids.newScanQueryBuilder;

public class DataServerResponseHandlerTest
{
  private static final SegmentDescriptor SEGMENT_1 = new SegmentDescriptor(Intervals.of("2003/2004"), "v0", 1);

  private static final HttpResponseHandler.TrafficCop NOOP_TRAFFIC_COP = new HttpResponseHandler.TrafficCop()
  {
    @Override
    public long resume(long chunkNum)
    {
      return 0;
    }

    @Override
    public void abort()
    {
      // Nothing to abort: this test drives the handler directly, without a channel.
    }
  };

  private ObjectMapper jsonMapper;
  private ResponseContext responseContext;

  @Before
  public void setUp()
  {
    jsonMapper = TestHelper.makeJsonMapper();
    responseContext = DefaultResponseContext.createEmpty();
  }

  private DataServerResponseHandler makeHandler(long timeoutMillis)
  {
    final ScanQuery query = newScanQueryBuilder()
        .dataSource("dataSource1")
        .intervals(new MultipleSpecificSegmentSpec(ImmutableList.of(SEGMENT_1)))
        .columns("__time")
        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
        .context(ImmutableMap.of("timeout", timeoutMillis))
        .build();
    return new DataServerResponseHandler(query, responseContext, jsonMapper);
  }

  /**
   * A buffered chunk holds a retain taken by InputStreamHolder.fromByteBuf. If the query fails before the consumer
   * reads that chunk, the handler is the only thing left that can release it.
   */
  @Test
  public void testFailedResponseReleasesBufferedChunks()
  {
    final DataServerResponseHandler handler = makeHandler(60_000L);

    final ClientResponse<InputStream> clientResponse = handler.handleResponse(
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK),
        NOOP_TRAFFIC_COP
    );

    // Stands in for an inbound chunk owned by NettyHttpClient, which releases its own reference once handleChunk
    // returns.
    final ByteBuf chunk = Unpooled.wrappedBuffer(StringUtils.toUtf8("[{\"timestamp\":\"2014-01-01T01:02:03Z\"}]"));
    Assert.assertEquals(1, chunk.refCnt());

    handler.handleChunk(clientResponse, new DefaultHttpContent(chunk), 1);
    Assert.assertEquals("retained while buffered", 2, chunk.refCnt());

    handler.exceptionCaught(clientResponse, new RuntimeException("transport failure"));

    Assert.assertEquals("released once the query failed", 1, chunk.refCnt());
    chunk.release();
  }

  /**
   * A FullHttpResponse arrives as both response and content, so its body has to be picked up from handleResponse
   * rather than waiting for a handleChunk that will never come.
   */
  @Test
  public void testFullResponseBodyIsRead() throws Exception
  {
    final DataServerResponseHandler handler = makeHandler(60_000L);

    final ByteBuf body = Unpooled.wrappedBuffer(StringUtils.toUtf8("[1,2]"));
    final ClientResponse<InputStream> clientResponse = handler.handleResponse(
        new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, body),
        NOOP_TRAFFIC_COP
    );

    handler.done(clientResponse);

    try (InputStream content = clientResponse.getObj()) {
      Assert.assertEquals("[1,2]", StringUtils.fromUtf8(content.readAllBytes()));
    }
  }

  /**
   * The timeout path reaches the same teardown through checkQueryTimeout rather than exceptionCaught.
   */
  @Test
  public void testTimedOutResponseReleasesBufferedChunks() throws Exception
  {
    final DataServerResponseHandler handler = makeHandler(50L);

    final ClientResponse<InputStream> clientResponse = handler.handleResponse(
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK),
        NOOP_TRAFFIC_COP
    );

    final ByteBuf chunk = Unpooled.wrappedBuffer(StringUtils.toUtf8("[{\"timestamp\":\"2014-01-01T01:02:03Z\"}]"));
    handler.handleChunk(clientResponse, new DefaultHttpContent(chunk), 1);
    Assert.assertEquals("retained while buffered", 2, chunk.refCnt());

    Thread.sleep(100L);

    // Any further interaction notices the elapsed timeout and tears the response down.
    Assert.assertThrows(
        Exception.class,
        () -> handler.handleChunk(clientResponse, new DefaultHttpContent(Unpooled.EMPTY_BUFFER), 2)
    );

    Assert.assertEquals("released once the query timed out", 1, chunk.refCnt());
    chunk.release();
  }
}
