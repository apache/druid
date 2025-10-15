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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.broker.BrokerClient;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SegmentLoadStatusFetcherTest
{
  private static final String TEST_DATASOURCE = "testDatasource";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private SegmentLoadStatusFetcher segmentLoadWaiter;

  private BrokerClient brokerClient;

  /**
   * Single version created, loaded after 3 attempts
   */
  @Test
  public void testSingleVersionWaitsForLoadCorrectly() throws Exception
  {
    brokerClient = mock(BrokerClient.class);

    String dummyString = "";
    when(brokerClient.submitSqlQuery(any(ClientSqlQuery.class))).thenReturn(Futures.immediateFuture(dummyString));
    doAnswer(new Answer<ListenableFuture<String>>()
    {
      int timesInvoked = 0;

      @Override
      public ListenableFuture<String> answer(InvocationOnMock invocation) throws Throwable
      {
        timesInvoked += 1;
        SegmentLoadStatusFetcher.VersionLoadStatus loadStatus = new SegmentLoadStatusFetcher.VersionLoadStatus(
            5,
            timesInvoked,
            0,
            5 - timesInvoked,
            0
        );
        String jsonResponse = OBJECT_MAPPER.writeValueAsString(loadStatus);
        return Futures.immediateFuture(jsonResponse);
      }
    }).when(brokerClient).submitSqlQuery(any(ClientSqlQuery.class));
    segmentLoadWaiter = new SegmentLoadStatusFetcher(
        brokerClient,
        OBJECT_MAPPER,
        "id",
        TEST_DATASOURCE,
        IntStream.range(0, 5).boxed().map(partitionNum -> createTestDataSegment("version1", partitionNum)).collect(Collectors.toSet()),
        false
    );
    segmentLoadWaiter.waitForSegmentsToLoad();

    verify(brokerClient, times(5)).submitSqlQuery(any(ClientSqlQuery.class));
  }

  @Test
  public void testMultipleVersionWaitsForLoadCorrectly() throws Exception
  {
    brokerClient = mock(BrokerClient.class);

    String dummyString = "";
    when(brokerClient.submitSqlQuery(any(ClientSqlQuery.class))).thenReturn(Futures.immediateFuture(dummyString));
    when(brokerClient.submitSqlQuery(any(ClientSqlQuery.class))).thenAnswer(new Answer<ListenableFuture<String>>()
    {
      int timesInvoked = 0;

      @Override
      public ListenableFuture<String> answer(InvocationOnMock invocation) throws Throwable
      {
        timesInvoked += 1;
        SegmentLoadStatusFetcher.VersionLoadStatus loadStatus = new SegmentLoadStatusFetcher.VersionLoadStatus(
            5,
            timesInvoked,
            0,
            5 - timesInvoked,
            0
        );
        String jsonResponse = OBJECT_MAPPER.writeValueAsString(loadStatus);
        return Futures.immediateFuture(jsonResponse);
      }
    });
    segmentLoadWaiter = new SegmentLoadStatusFetcher(
        brokerClient,
        OBJECT_MAPPER,
        "id",
        TEST_DATASOURCE,
        IntStream.range(0, 5).boxed().map(partitionNum -> createTestDataSegment("version1", partitionNum)).collect(Collectors.toSet()),
        false
    );
    segmentLoadWaiter.waitForSegmentsToLoad();

    verify(brokerClient, times(5)).submitSqlQuery(any(ClientSqlQuery.class));
  }

  @Test
  public void triggerCancellationFromAnotherThread() throws Exception
  {
    brokerClient = mock(BrokerClient.class);

    String dummyString = "";
    when(brokerClient.submitSqlQuery(any(ClientSqlQuery.class))).thenReturn(Futures.immediateFuture(dummyString));

    doAnswer(new Answer<ListenableFuture<String>>()
    {
      int timesInvoked = 0;

      @Override
      public ListenableFuture<String> answer(InvocationOnMock invocation) throws Throwable
      {
        // sleeping broker call to simulate a long-running query
        Thread.sleep(1000);
        timesInvoked++;
        SegmentLoadStatusFetcher.VersionLoadStatus loadStatus = new SegmentLoadStatusFetcher.VersionLoadStatus(
            5,
            timesInvoked,
            0,
            5 - timesInvoked,
            0
        );
        String jsonResponse = OBJECT_MAPPER.writeValueAsString(loadStatus);
        return Futures.immediateFuture(jsonResponse);
      }
    }).when(brokerClient).submitSqlQuery(any(ClientSqlQuery.class));
    segmentLoadWaiter = new SegmentLoadStatusFetcher(
        brokerClient,
        OBJECT_MAPPER,
        "id",
        TEST_DATASOURCE,
        IntStream.range(0, 5).boxed().map(partitionNum -> createTestDataSegment("version1", partitionNum)).collect(Collectors.toSet()),
        true
    );

    Thread t = new Thread(() -> segmentLoadWaiter.waitForSegmentsToLoad());
    t.start();
    // call close from main thread
    segmentLoadWaiter.close();
    t.join(1000);
    Assert.assertFalse(t.isAlive());

    Assert.assertTrue(segmentLoadWaiter.status().getState().isFinished());
    Assert.assertTrue(segmentLoadWaiter.status().getState() == SegmentLoadStatusFetcher.State.FAILED);
  }

  private static DataSegment createTestDataSegment(String version, int partitionNumber)
  {
    return new DataSegment(
        TEST_DATASOURCE,
        Intervals.ETERNITY,
        version,
        null,
        null,
        null,
        new NumberedShardSpec(partitionNumber, 1),
        0,
        0
    );
  }
}
