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

package org.apache.druid.server.system.query;

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.BadQueryContextException;
import org.apache.druid.query.Druids;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.SystemTableAccess;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.initialization.ServerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class SystemTableGatheringQuerySegmentWalkerTest
{
  private final QuerySegmentWalker delegate = Mockito.mock(QuerySegmentWalker.class);
  private final SystemTableQueryClient client = Mockito.mock(SystemTableQueryClient.class);
  private final SystemTableGatheringQuerySegmentWalker walker =
      new SystemTableGatheringQuerySegmentWalker(delegate, client, serverConfig(1_000L));

  /**
   * A native query on a system table has the local scope: it reads the Broker's rows through the Broker's walker,
   * under the server's timeout and scatter-gather limits.
   */
  @Test
  public void testLocalScopedSystemTableGoesToDelegateWithServerLimits()
  {
    final AtomicReference<Query<?>> delegated = new AtomicReference<>();
    Mockito.when(delegate.getQueryRunnerForIntervals(ArgumentMatchers.any(), ArgumentMatchers.any()))
           .thenReturn((QueryRunner<Object>) (queryPlus, responseContext) -> {
             delegated.set(queryPlus.getQuery());
             return Sequences.empty();
           });
    final ScanQuery query = scan(new SystemTableDataSource("server_properties").withAccess(SystemTableAccess.ALL_ROWS));

    walker.getQueryRunnerForIntervals(query, query.getIntervals())
          .run(QueryPlus.wrap((Query) query), ResponseContext.createEmpty())
          .toList();

    Assertions.assertEquals(1_000L, delegated.get().context().getTimeout());
    Assertions.assertTrue(delegated.get().context().has(QueryContextParameters.QUERY_FAIL_TIME));
    Mockito.verifyNoInteractions(client);
  }

  /**
   * A native query on a system table can't ask for a timeout above the server's maximum.
   */
  @Test
  public void testLocalScopedSystemTableRejectsTimeoutAboveServerMaximum()
  {
    Mockito.when(delegate.getQueryRunnerForIntervals(ArgumentMatchers.any(), ArgumentMatchers.any()))
           .thenReturn((QueryRunner<Object>) (queryPlus, responseContext) -> Sequences.empty());
    final ScanQuery query = Druids.ScanQueryBuilder
        .copy(scan(new SystemTableDataSource("server_properties").withAccess(SystemTableAccess.ALL_ROWS)))
        .context(QueryContext.ofMap(QueryContextParameters.TIMEOUT, 60_000L))
        .build();

    final QueryRunner<?> runner = walker.getQueryRunnerForIntervals(query, query.getIntervals());
    Assertions.assertThrows(
        BadQueryContextException.class,
        () -> runner.run(QueryPlus.wrap((Query) query), ResponseContext.createEmpty())
    );
  }

  /**
   * Queries that don't read a system table go to the Broker's walker unchanged.
   */
  @Test
  public void testQueryOnTableGoesToDelegateUnchanged()
  {
    final QueryRunner<Object> runner = (queryPlus, responseContext) -> Sequences.empty();
    Mockito.when(delegate.getQueryRunnerForIntervals(ArgumentMatchers.any(), ArgumentMatchers.any()))
           .thenReturn(runner);
    final ScanQuery query = scan(new TableDataSource("wikipedia"));

    Assertions.assertSame(runner, walker.getQueryRunnerForIntervals(query, query.getIntervals()));
    Mockito.verifyNoInteractions(client);
  }

  /**
   * A cluster-scoped system table is replaced with gathered rows when the query runs, then the closer runs.
   */
  @Test
  public void testClusterScopedSystemTableIsResolvedWhenQueryRuns() throws Exception
  {
    final ScanQuery query = scan(
        new SystemTableDataSource("server_properties")
            .withAccess(SystemTableAccess.ALL_ROWS)
            .withScope(SystemTableDataSource.Scope.CLUSTER)
    );
    final ScanQuery resolved = scan(InlineDataSource.fromIterable(List.of(), RowSignature.empty()));
    final AtomicBoolean closed = new AtomicBoolean();
    final Closeable closer = () -> closed.set(true);
    Mockito.when(client.resolve(ArgumentMatchers.<Query<Object>>any()))
           .thenAnswer(ignored -> new SystemTableQueryClient.ResolvedQuery<>(resolved, closer));
    final AtomicReference<Query<?>> delegated = new AtomicReference<>();
    Mockito.when(delegate.getQueryRunnerForIntervals(ArgumentMatchers.any(), ArgumentMatchers.any()))
           .thenAnswer(invocation -> {
             delegated.set(invocation.getArgument(0));
             return (QueryRunner<Object>) (queryPlus, responseContext) -> Sequences.empty();
           });

    final QueryRunner<?> runner = walker.getQueryRunnerForIntervals(query, query.getIntervals());
    Mockito.verifyNoInteractions(client, delegate);

    runner.run(QueryPlus.wrap((Query) query), ResponseContext.createEmpty()).toList();
    Assertions.assertSame(resolved, delegated.get());
    Assertions.assertTrue(closed.get());
  }

  /**
   * The gathered rows are released if the Broker's walker fails to plan the resolved query.
   */
  @Test
  public void testCloserRunsWhenDelegateFails()
  {
    final ScanQuery query = scan(
        new SystemTableDataSource("server_properties")
            .withAccess(SystemTableAccess.ALL_ROWS)
            .withScope(SystemTableDataSource.Scope.CLUSTER)
    );
    final AtomicBoolean closed = new AtomicBoolean();
    Mockito.when(client.resolve(ArgumentMatchers.<Query<Object>>any()))
           .thenAnswer(ignored -> new SystemTableQueryClient.ResolvedQuery<>(query, () -> closed.set(true)));
    Mockito.when(delegate.getQueryRunnerForIntervals(ArgumentMatchers.any(), ArgumentMatchers.any()))
           .thenThrow(new IllegalStateException("too many subquery rows"));

    final QueryRunner<?> runner = walker.getQueryRunnerForIntervals(query, query.getIntervals());
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> runner.run(QueryPlus.wrap((Query) query), ResponseContext.createEmpty())
    );
    Assertions.assertTrue(closed.get());
  }

  @Test
  public void testClusterScopedSystemTableCantBeQueriedForSegments()
  {
    final ScanQuery query = scan(
        new SystemTableDataSource("server_properties")
            .withAccess(SystemTableAccess.ALL_ROWS)
            .withScope(SystemTableDataSource.Scope.CLUSTER)
    );
    Assertions.assertThrows(DruidException.class, () -> walker.getQueryRunnerForSegments(query, List.of()));
    Mockito.verifyNoInteractions(client, delegate);
  }

  private static ServerConfig serverConfig(final long maxQueryTimeout)
  {
    final ServerConfig serverConfig = Mockito.mock(ServerConfig.class);
    Mockito.when(serverConfig.getDefaultQueryTimeout()).thenReturn(300_000L);
    Mockito.when(serverConfig.getMaxQueryTimeout()).thenReturn(maxQueryTimeout);
    Mockito.when(serverConfig.getMaxScatterGatherBytes()).thenReturn(Long.MAX_VALUE);
    return serverConfig;
  }

  private static ScanQuery scan(final org.apache.druid.query.DataSource dataSource)
  {
    return Druids.newScanQueryBuilder().dataSource(dataSource).eternityInterval().build();
  }
}
