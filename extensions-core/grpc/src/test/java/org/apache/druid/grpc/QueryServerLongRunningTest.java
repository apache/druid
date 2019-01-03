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

package org.apache.druid.grpc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.server.QueryManager;
import org.easymock.EasyMock;
import org.junit.Ignore;
import org.junit.Test;

import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;

public class QueryServerLongRunningTest
{
  @Ignore
  // Runs a perma-server for testing clients. Not intended to be a regular test
  @Test
  public void testServiceIT() throws Exception
  {
    final Result<TopNResultValue> result = new Result<>(
        DateTimes.of("2017-01-01T00:00:00Z"),
        new TopNResultValue(ImmutableList.of(ImmutableMap.of(
            "dim",
            1
        )))
    );
    final TopNQuery query = new TopNQueryBuilder()
        .dataSource("Test datasource")
        .intervals("2017-01-01/2018-01-01")
        .dimension("some dimension")
        .metric("some metric")
        .threshold(1)
        .aggregators(ImmutableList.of(new CountAggregatorFactory("some metric")))
        .build();
    final String id = "some id";
    final QuerySegmentWalker walker = EasyMock.createStrictMock(QuerySegmentWalker.class);
    EasyMock
        .expect(walker.getQueryRunnerForIntervals(
            EasyMock.anyObject(TopNQuery.class),
            EasyMock.eq(query.getIntervals())
        ))
        .andReturn((queryPlus, responseContext) -> Sequences.simple(ImmutableList.of(result)))
        .anyTimes();

    final QueryManager qm = EasyMock.createStrictMock(QueryManager.class);
    EasyMock.expect(qm.cancelQuery(EasyMock.anyString())).andReturn(true).anyTimes();
    EasyMock.replay(walker, qm);

    try (final Closer closer = Closer.create()) {
      final GrpcConfig config = new GrpcConfig();
      final QueryServer qs;
      try (final ServerSocket ss = new ServerSocket(0)) {
        ss.setReuseAddress(true);
        config.port = ss.getLocalPort();
        // I have no idea why this one needs its own config. But using the global config, even with a new port,
        // does *NOT* work
        qs = QueryServerTest.queryServer(qm, walker, config);
        // Minimize time between freeing socket and trying to start service
      }
      qs.start();
      closer.register(qs::stop);

      final Metadata metadata = new Metadata();
      metadata.put(qs.queryImpl.QUERY_ID_KEY, id);
      final ManagedChannel channel = ManagedChannelBuilder
          .forAddress("localhost", config.port)
          .usePlaintext()
          .build();
      closer.register(channel::shutdownNow);
      Thread.sleep(TimeUnit.DAYS.toMillis(1));
    }
    catch (Exception e) {
      throw new RE(e, "test failed");
    }
    EasyMock.verify(walker);
  }
}
