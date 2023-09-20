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
import com.google.common.collect.ImmutableSet;
import org.apache.druid.discovery.BrokerClient;
import org.apache.druid.java.util.http.client.Request;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SegmentLoadStatusFetcherTest
{
  private static final String TEST_DATASOURCE = "testDatasource";

  private SegmentLoadStatusFetcher segmentLoadWaiter;

  private BrokerClient brokerClient;

  /**
   * Single version created, loaded after 3 attempts
   */
  @Test
  public void testSingleVersionWaitsForLoadCorrectly() throws Exception
  {
    brokerClient = mock(BrokerClient.class);

    doReturn(mock(Request.class)).when(brokerClient).makeRequest(any(), anyString());
    doAnswer(new Answer<String>()
    {
      int timesInvoked = 0;

      @Override
      public String answer(InvocationOnMock invocation) throws Throwable
      {
        timesInvoked += 1;
        SegmentLoadStatusFetcher.VersionLoadStatus loadStatus = new SegmentLoadStatusFetcher.VersionLoadStatus(
            5,
            timesInvoked,
            0,
            5 - timesInvoked,
            0
        );
        return new ObjectMapper().writeValueAsString(loadStatus);
      }
    }).when(brokerClient).sendQuery(any());
    segmentLoadWaiter = new SegmentLoadStatusFetcher(
        brokerClient,
        new ObjectMapper(),
        "id",
        TEST_DATASOURCE,
        ImmutableSet.of("version1"),
        5,
        false
    );
    segmentLoadWaiter.waitForSegmentsToLoad();

    verify(brokerClient, times(5)).sendQuery(any());
  }

  @Test
  public void testMultipleVersionWaitsForLoadCorrectly() throws Exception
  {
    brokerClient = mock(BrokerClient.class);

    doReturn(mock(Request.class)).when(brokerClient).makeRequest(any(), anyString());
    doAnswer(new Answer<String>()
    {
      int timesInvoked = 0;

      @Override
      public String answer(InvocationOnMock invocation) throws Throwable
      {
        timesInvoked += 1;
        SegmentLoadStatusFetcher.VersionLoadStatus loadStatus = new SegmentLoadStatusFetcher.VersionLoadStatus(
            5,
            timesInvoked,
            0,
            5 - timesInvoked,
            0
        );
        return new ObjectMapper().writeValueAsString(loadStatus);
      }
    }).when(brokerClient).sendQuery(any());
    segmentLoadWaiter = new SegmentLoadStatusFetcher(
        brokerClient,
        new ObjectMapper(),
        "id",
        TEST_DATASOURCE,
        ImmutableSet.of("version1"),
        5,
        false
    );
    segmentLoadWaiter.waitForSegmentsToLoad();

    verify(brokerClient, times(5)).sendQuery(any());
  }

  @Test
  public void triggerCancellationFromAnotherThread() throws Exception
  {
    brokerClient = mock(BrokerClient.class);
    doReturn(mock(Request.class)).when(brokerClient).makeRequest(any(), anyString());
    doAnswer(new Answer<String>()
    {
      int timesInvoked = 0;

      @Override
      public String answer(InvocationOnMock invocation) throws Throwable
      {
        // sleeping broker call to simulate a long running query
        Thread.sleep(1000);
        timesInvoked++;
        SegmentLoadStatusFetcher.VersionLoadStatus loadStatus = new SegmentLoadStatusFetcher.VersionLoadStatus(
            5,
            timesInvoked,
            0,
            5 - timesInvoked,
            0
        );
        return new ObjectMapper().writeValueAsString(loadStatus);
      }
    }).when(brokerClient).sendQuery(any());
    segmentLoadWaiter = new SegmentLoadStatusFetcher(
        brokerClient,
        new ObjectMapper(),
        "id",
        TEST_DATASOURCE,
        ImmutableSet.of("version1"),
        5,
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

}
