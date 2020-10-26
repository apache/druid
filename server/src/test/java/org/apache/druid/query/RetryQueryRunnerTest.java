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

package org.apache.druid.query;

import org.apache.druid.client.DruidServer;
import org.apache.druid.client.SimpleServerView;
import org.apache.druid.client.TestHttpClient.SimpleServerManager;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.SegmentMissingException;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

public class RetryQueryRunnerTest extends QueryRunnerBasedOnClusteredClientTestBase
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testNoRetry()
  {
    prepareCluster(10);
    final Query<Result<TimeseriesResultValue>> query = timeseriesQuery(BASE_SCHEMA_INFO.getDataInterval());
    final RetryQueryRunner<Result<TimeseriesResultValue>> queryRunner = createQueryRunner(
        newRetryQueryRunnerConfig(1, false),
        query,
        () -> {}
    );
    final Sequence<Result<TimeseriesResultValue>> sequence = queryRunner.run(QueryPlus.wrap(query), responseContext());
    final List<Result<TimeseriesResultValue>> queryResult = sequence.toList();
    Assert.assertEquals(0, queryRunner.getTotalNumRetries());
    Assert.assertFalse(queryResult.isEmpty());
    Assert.assertEquals(expectedTimeseriesResult(10), queryResult);
  }

  @Test
  public void testRetryForMovedSegment()
  {
    prepareCluster(10);
    final Query<Result<TimeseriesResultValue>> query = timeseriesQuery(BASE_SCHEMA_INFO.getDataInterval());
    final RetryQueryRunner<Result<TimeseriesResultValue>> queryRunner = createQueryRunner(
        newRetryQueryRunnerConfig(1, true),
        query,
        () -> {
          // Let's move a segment
          dropSegmentFromServerAndAddNewServerForSegment(servers.get(0));
        }
    );
    final Sequence<Result<TimeseriesResultValue>> sequence = queryRunner.run(QueryPlus.wrap(query), responseContext());

    final List<Result<TimeseriesResultValue>> queryResult = sequence.toList();
    Assert.assertEquals(1, queryRunner.getTotalNumRetries());
    // Note that we dropped a segment from a server, but it's still announced in the server view.
    // As a result, we may get the full result or not depending on what server will get the retry query.
    // If we hit the same server, the query will return incomplete result.
    Assert.assertTrue(queryResult.size() == 9 || queryResult.size() == 10);
    Assert.assertEquals(expectedTimeseriesResult(queryResult.size()), queryResult);
  }

  @Test
  public void testRetryUntilWeGetFullResult()
  {
    prepareCluster(10);
    final Query<Result<TimeseriesResultValue>> query = timeseriesQuery(BASE_SCHEMA_INFO.getDataInterval());
    final RetryQueryRunner<Result<TimeseriesResultValue>> queryRunner = createQueryRunner(
        newRetryQueryRunnerConfig(100, false), // retry up to 100
        query,
        () -> {
          // Let's move a segment
          dropSegmentFromServerAndAddNewServerForSegment(servers.get(0));
        }
    );
    final Sequence<Result<TimeseriesResultValue>> sequence = queryRunner.run(QueryPlus.wrap(query), responseContext());

    final List<Result<TimeseriesResultValue>> queryResult = sequence.toList();
    Assert.assertTrue(0 < queryRunner.getTotalNumRetries());
    Assert.assertEquals(expectedTimeseriesResult(10), queryResult);
  }

  @Test
  public void testFailWithPartialResultsAfterRetry()
  {
    prepareCluster(10);
    final Query<Result<TimeseriesResultValue>> query = timeseriesQuery(BASE_SCHEMA_INFO.getDataInterval());
    final RetryQueryRunner<Result<TimeseriesResultValue>> queryRunner = createQueryRunner(
        newRetryQueryRunnerConfig(1, false),
        query,
        () -> dropSegmentFromServer(servers.get(0))
    );
    final Sequence<Result<TimeseriesResultValue>> sequence = queryRunner.run(QueryPlus.wrap(query), responseContext());

    expectedException.expect(SegmentMissingException.class);
    expectedException.expectMessage("No results found for segments");
    try {
      sequence.toList();
    }
    finally {
      Assert.assertEquals(1, queryRunner.getTotalNumRetries());
    }
  }

  /**
   * Drops a segment from the DruidServer. This method doesn't update the server view, but the server will stop
   * serving queries for the dropped segment.
   */
  private NonnullPair<DataSegment, QueryableIndex> dropSegmentFromServer(DruidServer fromServer)
  {
    final SimpleServerManager serverManager = httpClient.getServerManager(fromServer);
    Assert.assertNotNull(serverManager);
    return serverManager.dropSegment();
  }

  /**
   * Drops a segment from the DruidServer and update the server view.
   */
  private NonnullPair<DataSegment, QueryableIndex> unannounceSegmentFromServer(DruidServer fromServer)
  {
    final NonnullPair<DataSegment, QueryableIndex> pair = dropSegmentFromServer(fromServer);
    simpleServerView.unannounceSegmentFromServer(fromServer, pair.lhs);
    return pair;
  }

  /**
   * Drops a segment from the {@code fromServer} and creates a new server serving the dropped segment.
   * This method updates the server view.
   */
  private void dropSegmentFromServerAndAddNewServerForSegment(DruidServer fromServer)
  {
    final NonnullPair<DataSegment, QueryableIndex> pair = unannounceSegmentFromServer(fromServer);
    final DataSegment segmentToMove = pair.lhs;
    final QueryableIndex queryableIndexToMove = pair.rhs;
    addServer(
        SimpleServerView.createServer(11),
        segmentToMove,
        queryableIndexToMove
    );
  }

  private <T> RetryQueryRunner<T> createQueryRunner(
      RetryQueryRunnerConfig retryQueryRunnerConfig,
      Query<T> query,
      Runnable runnableAfterFirstAttempt
  )
  {
    final QueryRunner<T> baseRunner = cachingClusteredClient.getQueryRunnerForIntervals(query, query.getIntervals());
    return new RetryQueryRunner<>(
        baseRunner,
        cachingClusteredClient::getQueryRunnerForSegments,
        retryQueryRunnerConfig,
        objectMapper,
        runnableAfterFirstAttempt
    );
  }

  private static RetryQueryRunnerConfig newRetryQueryRunnerConfig(int numTries, boolean returnPartialResults)
  {
    return new RetryQueryRunnerConfig()
    {
      @Override
      public int getNumTries()
      {
        return numTries;
      }

      @Override
      public boolean isReturnPartialResults()
      {
        return returnPartialResults;
      }
    };
  }
}
