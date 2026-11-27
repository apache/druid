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
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.SystemTableAccess;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.server.LocalQuerySegmentWalker;
import org.apache.druid.server.initialization.ServerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class SystemTableQuerySegmentWalkerTest
{
  private final QuerySegmentWalker delegate = Mockito.mock(QuerySegmentWalker.class);
  private final LocalQuerySegmentWalker localWalker = Mockito.mock(LocalQuerySegmentWalker.class);
  private final SystemTableQuerySegmentWalker walker =
      new SystemTableQuerySegmentWalker(delegate, localWalker, new ServerConfig());

  @Test
  public void testQueryOnTableGoesToDelegate()
  {
    final QueryRunner<Object> runner = (queryPlus, responseContext) -> null;
    Mockito.when(delegate.getQueryRunnerForIntervals(ArgumentMatchers.any(), ArgumentMatchers.any()))
           .thenReturn(runner);
    final ScanQuery query = Druids.newScanQueryBuilder().dataSource("wikipedia").eternityInterval().build();

    Assertions.assertSame(runner, walker.getQueryRunnerForIntervals(query, query.getIntervals()));
    Mockito.verifyNoInteractions(localWalker);
  }

  /**
   * Queries on system tables run locally, under the server's timeout and scatter-gather limits.
   */
  @Test
  public void testQueryOnSystemTableRunsLocally()
  {
    final AtomicReference<Query<?>> executed = new AtomicReference<>();
    Mockito.when(localWalker.getQueryRunnerForIntervals(ArgumentMatchers.any(), ArgumentMatchers.any()))
           .thenReturn((queryPlus, responseContext) -> {
             executed.set(queryPlus.getQuery());
             return Sequences.empty();
           });
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(
                                      new SystemTableDataSource("server_properties")
                                          .withAccess(SystemTableAccess.ALL_ROWS)
                                  )
                                  .eternityInterval()
                                  .build();

    final QueryRunner<?> runner = walker.getQueryRunnerForIntervals(query, query.getIntervals());
    runner.run(QueryPlus.wrap((Query) query), ResponseContext.createEmpty()).toList();
    Assertions.assertEquals(
        new ServerConfig().getMaxScatterGatherBytes(),
        executed.get().context().getMaxScatterGatherBytes()
    );
    Assertions.assertTrue(executed.get().context().has(QueryContextParameters.QUERY_FAIL_TIME));
    Mockito.verifyNoInteractions(delegate);
  }

  /**
   * A system table that the server hasn't authorized can't be read.
   */
  @Test
  public void testUnauthorizedSystemTableIsRejected()
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(new SystemTableDataSource("server_properties"))
                                  .eternityInterval()
                                  .build();

    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> walker.getQueryRunnerForIntervals(query, query.getIntervals())
    );
    Assertions.assertEquals(DruidException.Category.DEFENSIVE, e.getCategory());
    Mockito.verifyNoInteractions(delegate, localWalker);
  }

  @Test
  public void testSystemTableCantBeQueriedForSegments()
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(
                                      new SystemTableDataSource("server_properties")
                                          .withAccess(SystemTableAccess.ALL_ROWS)
                                  )
                                  .eternityInterval()
                                  .build();

    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> walker.getQueryRunnerForSegments(query, List.of())
    );
    Assertions.assertEquals(DruidException.Category.UNSUPPORTED, e.getCategory());
    Mockito.verifyNoInteractions(delegate, localWalker);
  }

  @Test
  public void testSubqueryOnSystemTableIsRejected()
  {
    final Query<?> query = Druids.newTimeseriesQueryBuilder()
                                 .dataSource(
                                     new QueryDataSource(
                                         Druids.newScanQueryBuilder()
                                               .dataSource(
                                                   new SystemTableDataSource("server_properties")
                                                       .withAccess(SystemTableAccess.ALL_ROWS)
                                               )
                                               .eternityInterval()
                                               .build()
                                     )
                                 )
                                 .intervals(List.of(Intervals.ETERNITY))
                                 .aggregators(List.of(new CountAggregatorFactory("count")))
                                 .build();

    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> walker.getQueryRunnerForIntervals(query, query.getIntervals())
    );
    Assertions.assertEquals(DruidException.Category.UNSUPPORTED, e.getCategory());
    Mockito.verifyNoInteractions(delegate, localWalker);
  }
}
