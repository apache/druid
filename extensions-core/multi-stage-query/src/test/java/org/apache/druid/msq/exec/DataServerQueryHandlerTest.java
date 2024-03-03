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
import org.apache.druid.client.ImmutableSegmentLoadInfo;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.discovery.DataServerClient;
import org.apache.druid.discovery.DruidServiceTestUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.input.table.DataServerRequestDescriptor;
import org.apache.druid.msq.input.table.RichSegmentDescriptor;
import org.apache.druid.msq.querykit.InputNumberDataSource;
import org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.MapQueryToolChestWarehouse;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.rpc.RpcException;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;

import static org.apache.druid.msq.exec.DataServerQueryHandler.toSegmentDescriptorWithFullInterval;
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
public class DataServerQueryHandlerTest
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
  private static final DruidServerMetadata DRUID_SERVER_2 = new DruidServerMetadata(
      "name2",
      "host2:5050",
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
      0
  );
  private static final RichSegmentDescriptor SEGMENT_2 = new RichSegmentDescriptor(
      Intervals.of("2004/2005"),
      Intervals.of("2004/2005"),
      "v1",
      0
  );
  private DataServerClient dataServerClient1;
  private DataServerClient dataServerClient2;
  private CoordinatorClient coordinatorClient;
  private ScanQuery query;
  private DataServerQueryHandler target;

  @Before
  public void setUp()
  {
    dataServerClient1 = mock(DataServerClient.class);
    dataServerClient2 = mock(DataServerClient.class);
    coordinatorClient = mock(CoordinatorClient.class);
    query = newScanQueryBuilder()
        .dataSource(new InputNumberDataSource(1))
        .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2003/2004"))))
        .columns("__time", "cnt", "dim1", "dim2", "m1", "m2", "unique_dim1")
        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
        .context(ImmutableMap.of(QueryContexts.NUM_RETRIES_ON_MISSING_SEGMENTS_KEY, 1, MultiStageQueryContext.CTX_INCLUDE_SEGMENT_SOURCE, SegmentSource.REALTIME.toString()))
        .build();
    QueryToolChestWarehouse queryToolChestWarehouse = new MapQueryToolChestWarehouse(
        ImmutableMap.<Class<? extends Query>, QueryToolChest>builder()
                    .put(ScanQuery.class, new ScanQueryQueryToolChest(null, null))
                    .build()
    );
    target = spy(
        new DataServerQueryHandler(
            DATASOURCE1,
            new ChannelCounters(),
            mock(ServiceClientFactory.class),
            coordinatorClient,
            DruidServiceTestUtils.newJsonMapper(),
            queryToolChestWarehouse,
            Execs.scheduledSingleThreaded("query-cancellation-executor"),
            new DataServerRequestDescriptor(DRUID_SERVER_1, ImmutableList.of(SEGMENT_1, SEGMENT_2))
        )
    );
    doAnswer(invocationOnMock -> {
      ServiceLocation serviceLocation = invocationOnMock.getArgument(0);
      if (ServiceLocation.fromDruidServerMetadata(DRUID_SERVER_1).equals(serviceLocation)) {
        return dataServerClient1;
      } else if (ServiceLocation.fromDruidServerMetadata(DRUID_SERVER_2).equals(serviceLocation)) {
        return dataServerClient2;
      } else {
        throw new IllegalStateException();
      }
    }).when(target).makeDataServerClient(any());
  }

  @Test
  public void testFetchRowsFromServer()
  {
    ScanResultValue scanResultValue = new ScanResultValue(
        null,
        ImmutableList.of(),
        ImmutableList.of(
            ImmutableList.of("abc", "123"),
            ImmutableList.of("ghi", "456"),
            ImmutableList.of("xyz", "789")
        )
    );

    doReturn(Sequences.simple(ImmutableList.of(scanResultValue))).when(dataServerClient1).run(any(), any(), any(), any());

    DataServerQueryResult<Object[]> dataServerQueryResult = target.fetchRowsFromDataServer(
        query,
        ScanQueryFrameProcessor::mappingFunction,
        Closer.create()
    );

    Assert.assertTrue(dataServerQueryResult.getHandedOffSegments().getDescriptors().isEmpty());
    List<List<Object>> events = (List<List<Object>>) scanResultValue.getEvents();
    Yielder<Object[]> yielder = dataServerQueryResult.getResultsYielders().get(0);
    events.forEach(
        event -> {
          Assert.assertArrayEquals(event.toArray(), yielder.get());
          yielder.next(null);
        }
    );
  }

  @Test
  public void testOneSegmentRelocated()
  {
    ScanResultValue scanResultValue1 = new ScanResultValue(
        null,
        ImmutableList.of(),
        ImmutableList.of(
            ImmutableList.of("abc", "123"),
            ImmutableList.of("ghi", "456")
        )
    );

    doAnswer(invocation -> {
      ResponseContext responseContext = invocation.getArgument(1);
      responseContext.addMissingSegments(
          ImmutableList.of(
              toSegmentDescriptorWithFullInterval(SEGMENT_2)
          )
      );
      return Sequences.simple(ImmutableList.of(scanResultValue1));
    }).when(dataServerClient1).run(any(), any(), any(), any());

    ScanResultValue scanResultValue2 = new ScanResultValue(
        null,
        ImmutableList.of(),
        ImmutableList.of(
            ImmutableList.of("pit", "579"),
            ImmutableList.of("xyz", "897")
        )
    );

    doReturn(Sequences.simple(ImmutableList.of(scanResultValue2))).when(dataServerClient2).run(any(), any(), any(), any());

    doReturn(Futures.immediateFuture(Boolean.FALSE)).when(coordinatorClient).isHandoffComplete(DATASOURCE1, toSegmentDescriptorWithFullInterval(SEGMENT_2));
    doReturn(ImmutableList.of(
        new ImmutableSegmentLoadInfo(
            DataSegment.builder()
                       .interval(SEGMENT_2.getInterval())
                       .version(SEGMENT_2.getVersion())
                       .shardSpec(new NumberedShardSpec(SEGMENT_2.getPartitionNumber(), SEGMENT_2.getPartitionNumber()))
                       .dataSource(DATASOURCE1)
                       .size(1)
                       .build(),
        ImmutableSet.of(DRUID_SERVER_2)
        ))).when(coordinatorClient).fetchServerViewSegments(DATASOURCE1, ImmutableList.of(SEGMENT_2.getFullInterval()));

    DataServerQueryResult<Object[]> dataServerQueryResult = target.fetchRowsFromDataServer(
        query,
        ScanQueryFrameProcessor::mappingFunction,
        Closer.create()
    );

    Assert.assertTrue(dataServerQueryResult.getHandedOffSegments().getDescriptors().isEmpty());

    Yielder<Object[]> yielder1 = dataServerQueryResult.getResultsYielders().get(0);
    ((List<List<Object>>) scanResultValue1.getEvents()).forEach(
        event -> {
          Assert.assertArrayEquals(event.toArray(), yielder1.get());
          yielder1.next(null);
        }
    );

    Yielder<Object[]> yielder2 = dataServerQueryResult.getResultsYielders().get(1);
    ((List<List<Object>>) scanResultValue2.getEvents()).forEach(
        event -> {
          Assert.assertArrayEquals(event.toArray(), yielder2.get());
          yielder2.next(null);
        }
    );
  }

  @Test
  public void testHandoff()
  {
    doAnswer(invocation -> {
      ResponseContext responseContext = invocation.getArgument(1);
      responseContext.addMissingSegments(
          ImmutableList.of(
              toSegmentDescriptorWithFullInterval(SEGMENT_1),
              toSegmentDescriptorWithFullInterval(SEGMENT_2)
          )
      );
      return Sequences.empty();
    }).when(dataServerClient1).run(any(), any(), any(), any());
    doReturn(Futures.immediateFuture(Boolean.TRUE)).when(coordinatorClient).isHandoffComplete(DATASOURCE1, toSegmentDescriptorWithFullInterval(SEGMENT_1));
    doReturn(Futures.immediateFuture(Boolean.TRUE)).when(coordinatorClient).isHandoffComplete(DATASOURCE1, toSegmentDescriptorWithFullInterval(SEGMENT_2));

    DataServerQueryResult<Object[]> dataServerQueryResult = target.fetchRowsFromDataServer(
        query,
        ScanQueryFrameProcessor::mappingFunction,
        Closer.create()
    );

    Assert.assertEquals(ImmutableList.of(SEGMENT_1, SEGMENT_2), dataServerQueryResult.getHandedOffSegments().getDescriptors());
    Assert.assertTrue(dataServerQueryResult.getResultsYielders().isEmpty());
  }

  @Test
  public void testServerNotFoundWithoutHandoffShouldThrowException()
  {
    doThrow(
        new QueryInterruptedException(new RpcException("Could not connect to server"))
    ).when(dataServerClient1).run(any(), any(), any(), any());

    doReturn(Futures.immediateFuture(Boolean.FALSE)).when(coordinatorClient).isHandoffComplete(DATASOURCE1, toSegmentDescriptorWithFullInterval(SEGMENT_1));

    ScanQuery queryWithRetry = query.withOverriddenContext(ImmutableMap.of(QueryContexts.NUM_RETRIES_ON_MISSING_SEGMENTS_KEY, 3));

    Assert.assertThrows(DruidException.class, () ->
        target.fetchRowsFromDataServer(
            queryWithRetry,
            ScanQueryFrameProcessor::mappingFunction,
            Closer.create()
        )
    );

    verify(dataServerClient1, times(5)).run(any(), any(), any(), any());
  }

  @Test
  public void testServerNotFoundButHandoffShouldReturnWithStatus()
  {
    doThrow(
        new QueryInterruptedException(new RpcException("Could not connect to server"))
    ).when(dataServerClient1).run(any(), any(), any(), any());

    doReturn(Futures.immediateFuture(Boolean.TRUE)).when(coordinatorClient).isHandoffComplete(DATASOURCE1, toSegmentDescriptorWithFullInterval(SEGMENT_1));
    doReturn(Futures.immediateFuture(Boolean.TRUE)).when(coordinatorClient).isHandoffComplete(DATASOURCE1, toSegmentDescriptorWithFullInterval(SEGMENT_2));

    DataServerQueryResult<Object[]> dataServerQueryResult = target.fetchRowsFromDataServer(
        query,
        ScanQueryFrameProcessor::mappingFunction,
        Closer.create()
    );

    Assert.assertEquals(ImmutableList.of(SEGMENT_1, SEGMENT_2), dataServerQueryResult.getHandedOffSegments().getDescriptors());
    Assert.assertTrue(dataServerQueryResult.getResultsYielders().isEmpty());
  }

  @Test
  public void testQueryFail()
  {
    SegmentDescriptor segmentDescriptorWithFullInterval = toSegmentDescriptorWithFullInterval(SEGMENT_1);
    doAnswer(invocation -> {
      ResponseContext responseContext = invocation.getArgument(1);
      responseContext.addMissingSegments(ImmutableList.of(segmentDescriptorWithFullInterval));
      return Sequences.empty();
    }).when(dataServerClient1).run(any(), any(), any(), any());
    doReturn(Futures.immediateFuture(Boolean.FALSE)).when(coordinatorClient).isHandoffComplete(DATASOURCE1, segmentDescriptorWithFullInterval);

    Assert.assertThrows(DruidException.class, () ->
        target.fetchRowsFromDataServer(
            query,
            ScanQueryFrameProcessor::mappingFunction,
            Closer.create()
        )
    );
  }
}
