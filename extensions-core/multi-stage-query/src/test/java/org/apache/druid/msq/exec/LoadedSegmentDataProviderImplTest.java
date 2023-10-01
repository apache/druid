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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.input.table.RichSegmentDescriptor;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.rpc.ServiceLocations;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.util.Arrays;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

@RunWith(MockitoJUnitRunner.class)
public class LoadedSegmentDataProviderImplTest
{
  private final String TEST_HOST = "indexer";
  private final int TEST_PORT = 9092;
  @Mock
  ServiceClientFactory serviceClientFactory;
  @Mock
  ServiceClient serviceClient;

  @Mock
  CoordinatorClient coordinatorClient;
  final RichSegmentDescriptor testSegmentDescriptor = new RichSegmentDescriptor(
      Intervals.ETERNITY,
      Intervals.ETERNITY,
      "v1",
      1,
      ImmutableSet.of(new DruidServerMetadata("indexer", TEST_HOST + ":" + TEST_PORT, null, 1, ServerType.REALTIME, "tier", 2)));

  final LoadedSegmentDataProviderImpl target = new LoadedSegmentDataProviderImpl(
      QueryRunnerTestHelper.DATA_SOURCE, new ChannelCounters(), serviceClientFactory, coordinatorClient, new ObjectMapper()
  );

  @Before
  public void setUp() throws Exception
  {
    serviceClientFactory = (serviceName, serviceLocator, retryPolicy) -> {
      Assert.assertEquals(
          FutureUtils.getUnchecked(serviceLocator.locate(), true),
          ServiceLocations.forLocation(new ServiceLocation(TEST_HOST, TEST_PORT, -1, ""))
      );
      return serviceClient;
    };
    ScanResultValue scanResultValue = new ScanResultValue(
        null,
        ImmutableList.of(),
        ImmutableList.of(
            ImmutableList.of("abc", "123"),
            ImmutableList.of("ghi", "456"),
            ImmutableList.of("xyz", "789")
        )
    );

    doAnswer(
        invocation -> {
//          ResponseContext responseContext = invocation.getArgument(1);
          return new ByteArrayInputStream(new ObjectMapper().writeValueAsBytes(scanResultValue));
        }
    ).when(serviceClient).request(any(), any());
  }

  @Test
  public void fetchRowsFromDataServerWithScanQuery()
  {
    ScanQuery query = new ScanQuery(
        new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
        new LegacySegmentSpec(Intervals.of("2011-01-12/2011-01-14")),
        VirtualColumns.EMPTY,
        ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST,
        0,
        0,
        3,
        ScanQuery.Order.NONE,
        null,
        null,
        Arrays.asList("dim1", "dim2"),
        null,
        null
    );

//    Sequence<Object[]> sequence = target.fetchRowsFromDataServer(
//        query,
//        ScanQueryFrameProcessor::mappingFunction,
//        Closer.create()
//    );
//
//    Assert.assertEquals(sequence.toList(), List.of(
//        new Object[] {"abc", "123"},
//        new Object[] {"ghi", "456"},
//        new Object[] {"xyz", "789"}
//    ));
  }
}