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

package org.apache.druid.msq.exec;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.discovery.DataServerClient;
import org.apache.druid.discovery.DruidServiceTestUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.input.table.RichSegmentDescriptor;
import org.apache.druid.msq.querykit.InputNumberDataSource;
import org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor;
import org.apache.druid.query.MapQueryToolChestWarehouse;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.rpc.RpcException;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.List;

import static org.apache.druid.query.Druids.newScanQueryBuilder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class LoadedSegmentDataProviderTest
{
  private static final String DATASOURCE1 = "dataSource1";
  private static final DruidServerMetadata DRUID_SERVER_1 = new DruidServerMetadata(
      "name1",
      "host1:5050",
      null,
      100L,
      ServerType.REALTIME,
      "tier1",
      0
  );
  private static final RichSegmentDescriptor SEGMENT_1 = new RichSegmentDescriptor(
      Intervals.of("2003/2004"),
      Intervals.of("2003/2004"),
      "v1",
      1,
      ImmutableSet.of(DRUID_SERVER_1)
  );
  private DataServerClient dataServerClient;
  private CoordinatorClient coordinatorClient;
  private ScanResultValue scanResultValue;
  private ScanQuery query;
  private LoadedSegmentDataProvider target;

  @Before
  public void setUp()
  {
    dataServerClient = mock(DataServerClient.class);
    coordinatorClient = mock(CoordinatorClient.class);
    scanResultValue = new ScanResultValue(
        null,
        ImmutableList.of(),
        ImmutableList.of(
            ImmutableList.of("abc", "123"),
            ImmutableList.of("ghi", "456"),
            ImmutableList.of("xyz", "789")
        )
    );
    query = newScanQueryBuilder()
        .dataSource(new InputNumberDataSource(1))
        .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2003/2004"))))
        .columns("__time", "cnt", "dim1", "dim2", "m1", "m2", "unique_dim1")
        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
        .context(ImmutableMap.of(QueryContexts.NUM_RETRIES_ON_MISSING_SEGMENTS_KEY, 1))
        .build();
    QueryToolChestWarehouse queryToolChestWarehouse = new MapQueryToolChestWarehouse(
        ImmutableMap.<Class<? extends Query>, QueryToolChest>builder()
                    .put(ScanQuery.class, new ScanQueryQueryToolChest(null, null))
                    .build()
    );
    target = spy(
        new LoadedSegmentDataProvider(
            DATASOURCE1,
            new ChannelCounters(),
            mock(ServiceClientFactory.class),
            coordinatorClient,
            DruidServiceTestUtils.newJsonMapper(),
            queryToolChestWarehouse,
            Execs.scheduledSingleThreaded("query-cancellation-executor")
        )
    );
    doReturn(dataServerClient).when(target).makeDataServerClient(any());
  }

  @Test
  public void testFetchRowsFromServer() throws IOException
  {
    doReturn(Sequences.simple(ImmutableList.of(scanResultValue))).when(dataServerClient).run(any(), any(), any(), any());

    Pair<LoadedSegmentDataProvider.DataServerQueryStatus, Yielder<Object[]>> dataServerQueryStatusYielderPair = target.fetchRowsFromDataServer(
        query,
        SEGMENT_1,
        ScanQueryFrameProcessor::mappingFunction,
        Closer.create()
    );

    Assert.assertEquals(LoadedSegmentDataProvider.DataServerQueryStatus.SUCCESS, dataServerQueryStatusYielderPair.lhs);
    List<List<Object>> events = (List<List<Object>>) scanResultValue.getEvents();
    Yielder<Object[]> yielder = dataServerQueryStatusYielderPair.rhs;
    events.forEach(
        event -> {
          Assert.assertArrayEquals(event.toArray(), yielder.get());
          yielder.next(null);
        }
    );
  }

  @Test
  public void testHandoff() throws IOException
  {
    doAnswer(invocation -> {
      ResponseContext responseContext = invocation.getArgument(1);
      responseContext.addMissingSegments(ImmutableList.of(SEGMENT_1));
      return Sequences.empty();
    }).when(dataServerClient).run(any(), any(), any(), any());
    doReturn(Futures.immediateFuture(Boolean.TRUE)).when(coordinatorClient).isHandoffComplete(DATASOURCE1, SEGMENT_1);

    Pair<LoadedSegmentDataProvider.DataServerQueryStatus, Yielder<Object[]>> dataServerQueryStatusYielderPair = target.fetchRowsFromDataServer(
        query,
        SEGMENT_1,
        ScanQueryFrameProcessor::mappingFunction,
        Closer.create()
    );

    Assert.assertEquals(LoadedSegmentDataProvider.DataServerQueryStatus.HANDOFF, dataServerQueryStatusYielderPair.lhs);
    Assert.assertNull(dataServerQueryStatusYielderPair.rhs);
  }

  @Test
  public void testServerNotFoundWithoutHandoffShouldThrowException()
  {
    doThrow(
        new QueryInterruptedException(new RpcException("Could not connect to server"))
    ).when(dataServerClient).run(any(), any(), any(), any());

    doReturn(Futures.immediateFuture(Boolean.FALSE)).when(coordinatorClient).isHandoffComplete(DATASOURCE1, SEGMENT_1);

    ScanQuery queryWithRetry = query.withOverriddenContext(ImmutableMap.of(QueryContexts.NUM_RETRIES_ON_MISSING_SEGMENTS_KEY, 3));

    Assert.assertThrows(DruidException.class, () ->
        target.fetchRowsFromDataServer(
            queryWithRetry,
            SEGMENT_1,
            ScanQueryFrameProcessor::mappingFunction,
            Closer.create()
        )
    );

    verify(dataServerClient, times(3)).run(any(), any(), any(), any());
  }

  @Test
  public void testServerNotFoundButHandoffShouldReturnWithStatus() throws IOException
  {
    doThrow(
        new QueryInterruptedException(new RpcException("Could not connect to server"))
    ).when(dataServerClient).run(any(), any(), any(), any());

    doReturn(Futures.immediateFuture(Boolean.TRUE)).when(coordinatorClient).isHandoffComplete(DATASOURCE1, SEGMENT_1);

    Pair<LoadedSegmentDataProvider.DataServerQueryStatus, Yielder<Object[]>> dataServerQueryStatusYielderPair = target.fetchRowsFromDataServer(
        query,
        SEGMENT_1,
        ScanQueryFrameProcessor::mappingFunction,
        Closer.create()
    );

    Assert.assertEquals(LoadedSegmentDataProvider.DataServerQueryStatus.HANDOFF, dataServerQueryStatusYielderPair.lhs);
    Assert.assertNull(dataServerQueryStatusYielderPair.rhs);
  }

  @Test
  public void testQueryFail()
  {
    doAnswer(invocation -> {
      ResponseContext responseContext = invocation.getArgument(1);
      responseContext.addMissingSegments(ImmutableList.of(SEGMENT_1));
      return Sequences.empty();
    }).when(dataServerClient).run(any(), any(), any(), any());
    doReturn(Futures.immediateFuture(Boolean.FALSE)).when(coordinatorClient).isHandoffComplete(DATASOURCE1, SEGMENT_1);

    Assert.assertThrows(IOE.class, () ->
        target.fetchRowsFromDataServer(
            query,
            SEGMENT_1,
            ScanQueryFrameProcessor::mappingFunction,
            Closer.create()
        )
    );
  }
}
