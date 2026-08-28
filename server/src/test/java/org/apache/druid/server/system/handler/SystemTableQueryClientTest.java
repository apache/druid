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

package org.apache.druid.server.system.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.client.DirectDruidClientFactory;
import org.apache.druid.client.DruidServer;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.guava.LazySequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FilteredDataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.operator.ScanOperatorFactory;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AllowAllAuthorizer;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.NoopEscalator;
import org.apache.druid.server.system.SystemTableNotLeaderException;
import org.apache.druid.server.system.table.ServerPropertiesTableDescriptor;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.apache.druid.server.system.table.TaskTableDescriptor;
import org.joda.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class SystemTableQueryClientTest
{
  @RegisterExtension
  public static final QueryStackTests.ConglomerateExtension CONGLOMERATE =
      new QueryStackTests.ConglomerateExtension();

  private static final AuthenticationResult AUTHENTICATION_RESULT =
      new AuthenticationResult("test", "allow", "test", null);

  @Test
  public void testNodeRowsAreLazyAndFlattenScanResultBatches()
  {
    final AtomicInteger runnerCalls = new AtomicInteger();
    final ScanQuery nodeQuery = Druids.newScanQueryBuilder()
                                            .dataSource(new SystemTableDataSource("test"))
                                            .eternityInterval()
                                            .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                            .build();
    final QueryRunner<ScanResultValue> nodeRunner = (queryPlus, responseContext) -> {
      runnerCalls.incrementAndGet();
      final Sequence<ScanResultValue> result = Sequences.simple(
          List.of(
              new ScanResultValue(
                  null,
                  List.of("task_id"),
                  List.of(List.of("task-a"), List.of("task-b"))
              ),
              new ScanResultValue(
                  null,
                  List.of("task_id"),
                  List.of((Object) new Object[]{"task-c"})
              )
          )
      );
      return result;
    };

    final Iterable<Object[]> rows = SystemTableQueryClient.scanNodeRows(
        List.of(nodeRunner),
        nodeQuery,
        ResponseContext.createEmpty()
    );

    Assertions.assertEquals(0, runnerCalls.get());

    final Iterator<Object[]> iterator = rows.iterator();
    Assertions.assertEquals(1, runnerCalls.get());
    Assertions.assertArrayEquals(new Object[]{"task-a"}, iterator.next());

    final List<Object[]> remainingRows = new ArrayList<>();
    iterator.forEachRemaining(remainingRows::add);
    Assertions.assertEquals(2, remainingRows.size());
    Assertions.assertArrayEquals(new Object[]{"task-b"}, remainingRows.get(0));
    Assertions.assertArrayEquals(new Object[]{"task-c"}, remainingRows.get(1));
    Assertions.assertThrows(IllegalStateException.class, rows::iterator);
  }

  /** All node HTTP requests start before the merged result waits for or consumes the first response. */
  @Test
  public void testNodeRequestsStartConcurrently()
  {
    final AtomicInteger runnerCalls = new AtomicInteger();
    final ScanQuery nodeQuery = Druids.newScanQueryBuilder()
                                           .dataSource(new SystemTableDataSource("test"))
                                           .eternityInterval()
                                           .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                           .build();
    final QueryRunner<ScanResultValue> firstRunner = (queryPlus, responseContext) -> {
      runnerCalls.incrementAndGet();
      return Sequences.simple(List.of(scanResult(new Object[]{"first"}))).map(result -> {
        Assertions.assertEquals(2, runnerCalls.get());
        return result;
      });
    };
    final QueryRunner<ScanResultValue> secondRunner = (queryPlus, responseContext) -> {
      runnerCalls.incrementAndGet();
      return Sequences.simple(List.of(scanResult(new Object[]{"second"})));
    };

    final Iterable<Object[]> rows = SystemTableQueryClient.scanNodeRows(
        List.of(firstRunner, secondRunner),
        nodeQuery,
        ResponseContext.createEmpty()
    );

    Assertions.assertEquals(0, runnerCalls.get());
    Assertions.assertNotNull(rows.iterator().next());
    Assertions.assertEquals(2, runnerCalls.get());
  }

  /**
   * Native query transport for:
   *
   * <pre>{@code
   * SELECT property, value FROM sys.server_properties
   * }</pre>
   *
   * A ready node must yield rows without waiting for a later node's response sequence to initialize.
   */
  @Test
  public void testReadyNodeDoesNotWaitForLaterNodeInitialization() throws Exception
  {
    final CompletableFuture<Void> secondNodeInitialization = new CompletableFuture<>();
    final CountDownLatch releaseSecondNode = new CountDownLatch(1);
    final AtomicReference<Iterator<Object[]>> rowIterator = new AtomicReference<>();
    final ExecutorService consumerExecutor = Executors.newSingleThreadExecutor();
    final ScanQuery nodeQuery = Druids.newScanQueryBuilder()
                                           .dataSource(
                                               new SystemTableDataSource(
                                                   ServerPropertiesTableDescriptor.TABLE_NAME
                                               )
                                           )
                                           .eternityInterval()
                                           .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                           .build();
    final QueryRunner<ScanResultValue> readyRunner = (queryPlus, responseContext) -> Sequences.simple(
        List.of(scanResult(new Object[]{"first"}, new Object[]{"first-remainder"}))
    );
    final QueryRunner<ScanResultValue> delayedRunner = (queryPlus, responseContext) -> new LazySequence<>(() -> {
      secondNodeInitialization.complete(null);
      try {
        releaseSecondNode.await();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      return Sequences.simple(List.of(scanResult(new Object[]{"second"})));
    });
    final Iterable<Object[]> rows = SystemTableQueryClient.scanNodeRows(
        List.of(readyRunner, delayedRunner),
        nodeQuery,
        ResponseContext.createEmpty()
    );

    try {
      final CompletableFuture<Object[]> firstRow = CompletableFuture.supplyAsync(
          () -> {
            final Iterator<Object[]> iterator = rows.iterator();
            rowIterator.set(iterator);
            return iterator.next();
          },
          consumerExecutor
      );

      CompletableFuture.anyOf(firstRow, secondNodeInitialization).get(10, TimeUnit.SECONDS);
      Assertions.assertTrue(firstRow.isDone());
      Assertions.assertFalse(secondNodeInitialization.isDone());
      Assertions.assertArrayEquals(new Object[]{"first"}, firstRow.get());

      releaseSecondNode.countDown();
      final List<Object[]> remainingRows = CompletableFuture.supplyAsync(
          () -> {
            final List<Object[]> result = new ArrayList<>();
            rowIterator.get().forEachRemaining(result::add);
            return result;
          },
          consumerExecutor
      ).get(10, TimeUnit.SECONDS);
      Assertions.assertEquals(2, remainingRows.size());
      Assertions.assertArrayEquals(new Object[]{"first-remainder"}, remainingRows.get(0));
      Assertions.assertArrayEquals(new Object[]{"second"}, remainingRows.get(1));
    }
    finally {
      releaseSecondNode.countDown();
      consumerExecutor.shutdownNow();
    }
  }

  /** A node selected at the Broker's own address executes in-process without creating an HTTP client. */
  @Test
  public void testBrokerNodeExecutesLocallyWithoutHttp()
  {
    final SystemTableDescriptor descriptor = new TestSystemTableDescriptor();
    final DiscoveryDruidNode brokerNode = testNode();
    final SystemTableNodeLocator nodeLocator = Mockito.mock(SystemTableNodeLocator.class);
    final SystemTableNode node = new SystemTableNode(brokerNode);
    node.addNodeRole(brokerNode.getNodeRole());
    Mockito.when(nodeLocator.locate(Mockito.eq(descriptor), ArgumentMatchers.any())).thenReturn(List.of(node));

    final DirectDruidClientFactory directClientFactory = Mockito.mock(DirectDruidClientFactory.class);
    final SystemTableQueryHandler localQueryHandler = Mockito.mock(SystemTableQueryHandler.class);
    final AuthenticationResult escalatedAuthenticationResult =
        new AuthenticationResult("system", "allow", "system", null);
    final Escalator escalator = Mockito.mock(Escalator.class);
    Mockito.when(escalator.createEscalatedAuthenticationResult()).thenReturn(escalatedAuthenticationResult);
    Mockito.doAnswer(
        ignored -> (QueryRunner<ScanResultValue>) (queryPlus, responseContext) ->
            Sequences.simple(List.of(scanResult(new Object[]{"local"})))
    ).when(localQueryHandler).createRunner(
        ArgumentMatchers.any(),
        Mockito.same(escalatedAuthenticationResult),
        Mockito.eq(true)
    );

    final QuerySegmentWalker querySegmentWalker = Mockito.mock(QuerySegmentWalker.class);
    Mockito.when(querySegmentWalker.getQueryRunnerForIntervals(ArgumentMatchers.any(), ArgumentMatchers.any()))
           .thenAnswer(ignored -> passthroughRunner(descriptor.getRowSignature()));
    final SystemTableQueryClient client = new SystemTableQueryClient(
        nodeLocator,
        directClientFactory,
        Mockito.mock(QueryScheduler.class),
        querySegmentWalker,
        Map.of(descriptor.getTableName(), descriptor),
        new AuthorizerMapper(Map.of("allow", new AllowAllAuthorizer(null))),
        localQueryHandler,
        escalator,
        brokerNode.getDruidNode()
    );
    final ScanQuery query = query(descriptor, Collections.emptyMap());

    final List<ScanResultValue> results = client.createRunner(query, AUTHENTICATION_RESULT, false)
                                                .run(QueryPlus.wrap(query), ResponseContext.createEmpty())
                                                .toList();

    Assertions.assertEquals(1, results.size());
    final List<?> events = (List<?>) results.get(0).getEvents();
    Assertions.assertEquals(1, events.size());
    Assertions.assertArrayEquals(new Object[]{"local"}, (Object[]) events.get(0));
    Mockito.verifyNoInteractions(directClientFactory);
    Mockito.verify(localQueryHandler).createRunner(
        ArgumentMatchers.any(),
        Mockito.same(escalatedAuthenticationResult),
        Mockito.eq(true)
    );
  }

  /** Delayed node responses verify concurrent fanout through the real {@link DirectDruidClient} transport path. */
  @Test
  public void testDirectClientsStartRequestsBeforeWaitingForDelayedResponses() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final DelayedHttpClient httpClient = new DelayedHttpClient(2);
    final ScheduledExecutorService cancellationExecutor = Executors.newSingleThreadScheduledExecutor();
    final ExecutorService consumerExecutor = Executors.newSingleThreadExecutor();
    try {
      final ScanQuery nodeQuery = Druids.newScanQueryBuilder()
                                             .dataSource(new SystemTableDataSource("test"))
                                             .eternityInterval()
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .context(Map.of(DirectDruidClient.QUERY_FAIL_TIME, Long.MAX_VALUE))
                                             .build();
      final List<QueryRunner<ScanResultValue>> nodeRunners = List.of(
          directClient(objectMapper, httpClient, "localhost:8081", cancellationExecutor),
          directClient(objectMapper, httpClient, "localhost:8082", cancellationExecutor)
      );
      final Iterable<Object[]> rows = SystemTableQueryClient.scanNodeRows(
          nodeRunners,
          nodeQuery,
          DirectDruidClient.makeResponseContextForQuery()
      );

      final CompletableFuture<List<Object[]>> consumedRows = CompletableFuture.supplyAsync(
          () -> {
            final List<Object[]> result = new ArrayList<>();
            rows.forEach(result::add);
            return result;
          },
          consumerExecutor
      );

      Assertions.assertTrue(httpClient.awaitRequests());
      httpClient.respond(
          0,
          objectMapper.writeValueAsBytes(List.of(scanResult(new Object[]{"first"})))
      );
      httpClient.respond(
          1,
          objectMapper.writeValueAsBytes(List.of(scanResult(new Object[]{"second"})))
      );

      final List<Object[]> result = consumedRows.get(10, TimeUnit.SECONDS);
      Assertions.assertEquals(2, result.size());
      Assertions.assertArrayEquals(new Object[]{"first"}, result.get(0));
      Assertions.assertArrayEquals(new Object[]{"second"}, result.get(1));
    }
    finally {
      consumerExecutor.shutdownNow();
      cancellationExecutor.shutdownNow();
    }
  }

  /** Closing a partial result closes its initialized transport and cancels every concurrently started node query. */
  @Test
  public void testClosingPartialResultCleansUpInitializedDirectClientTransport() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final DelayedHttpClient httpClient = new DelayedHttpClient(2);
    final ScheduledExecutorService cancellationExecutor = Executors.newSingleThreadScheduledExecutor();
    final ExecutorService consumerExecutor = Executors.newSingleThreadExecutor();
    final QueryScheduler queryScheduler = Mockito.mock(QueryScheduler.class);
    final SystemTableDescriptor descriptor = new TestSystemTableDescriptor();
    try {
      final SystemTableQueryClient client = makeDirectClientBackedClient(
          descriptor,
          List.of(testNode(8081), testNode(8082)),
          objectMapper,
          httpClient,
          cancellationExecutor,
          queryScheduler
      );
      final ScanQuery query = query(descriptor, Collections.emptyMap());
      final CompletableFuture<Yielder<ScanResultValue>> yielderFuture = CompletableFuture.supplyAsync(
          () -> Yielders.each(
              client.createRunner(query, AUTHENTICATION_RESULT, false)
                    .run(QueryPlus.wrap(query), ResponseContext.createEmpty())
          ),
          consumerExecutor
      );

      Assertions.assertTrue(httpClient.awaitRequests());
      final CloseTrackingInputStream firstResponse = httpClient.respond(
          0,
          objectMapper.writeValueAsBytes(
              List.of(scanResult(new Object[]{"first"}), scanResult(new Object[]{"first-extra"}))
          )
      );
      httpClient.respond(
          1,
          objectMapper.writeValueAsBytes(
              List.of(scanResult(new Object[]{"second"}), scanResult(new Object[]{"second-extra"}))
          )
      );
      final Yielder<ScanResultValue> yielder = yielderFuture.get(10, TimeUnit.SECONDS);
      Assertions.assertFalse(yielder.isDone());

      yielder.close();

      Assertions.assertTrue(firstResponse.isClosed());
      // The second response sequence was deliberately not initialized. Its remote work is stopped by query-id
      // cancellation below rather than by opening and closing its response stream during Broker cleanup.
      Mockito.verify(queryScheduler, Mockito.times(2)).cancelQuery(
          ArgumentMatchers.argThat(queryId -> queryId.startsWith(SystemTableDataSource.NODE_QUERY_ID_PREFIX))
      );
    }
    finally {
      consumerExecutor.shutdownNow();
      cancellationExecutor.shutdownNow();
    }
  }

  /**
   * Native query form of:
   *
   * <pre>{@code
   * SELECT COUNT(*)
   * FROM
   * (
   *   SELECT
   *     task_id,
   *     COUNT(*) AS task_count
   *   FROM sys.tasks
   *   GROUP BY task_id
   * )
   * WHERE task_count > 0
   * }</pre>
   */
  @Test
  public void testResolvesSystemTableInsideQueryDataSource()
  {
    final SystemTableDescriptor descriptor = new TestSystemTableDescriptor();
    final ScanQuery innerQuery = query(descriptor, Collections.emptyMap());

    assertWrappedSystemTableIsResolved(
        descriptor,
        new QueryDataSource(innerQuery)
    );
  }

  /**
   * Native query form of:
   *
   * <pre>{@code
   * SELECT task_id
   * FROM sys.tasks
   * WHERE type = 'index_parallel'
   * }</pre>
   */
  @Test
  public void testResolvesSystemTableInsideFilteredDataSource()
  {
    final SystemTableDescriptor descriptor = new TestSystemTableDescriptor();

    assertWrappedSystemTableIsResolved(
        descriptor,
        FilteredDataSource.create(
            new SystemTableDataSource(descriptor.getTableName()),
            new SelectorDimFilter("type", "index_parallel", null)
        )
    );
  }

  /**
   * Native query form of:
   *
   * <pre>{@code
   * SELECT l.value
   * FROM sys.server_properties AS l
   * JOIN sys.server_properties AS r ON l.property = r.property
   * WHERE r.value = 'right'
   * }</pre>
   *
   * The root filter uses the right-side join prefix and must remain on the Broker instead of being copied into either
   * node-local system table scan.
   */
  @Test
  public void testSelfJoinRootFilterIsNotPushedIntoSystemTableLeaves()
  {
    final SystemTableDescriptor descriptor = new ServerPropertiesTableDescriptor();
    final List<ScanQuery> nodeQueries = captureNodeQueries(
        descriptor,
        selfJoin(descriptor),
        new SelectorDimFilter("j.value", "right", null)
    );

    Assertions.assertEquals(2, nodeQueries.size());
    Assertions.assertNull(nodeQueries.get(0).getFilter());
    Assertions.assertNull(nodeQueries.get(1).getFilter());
  }

  /**
   * Native query form of:
   *
   * <pre>{@code
   * SELECT l.value
   * FROM sys.server_properties AS l
   * JOIN sys.server_properties AS r ON l.property = r.property
   * WHERE l.value = 'left'
   * }</pre>
   *
   * Even an unprefixed root filter belongs to the join result. Copying it into every system table leaf incorrectly
   * filters the right side of the join.
   */
  @Test
  public void testUnprefixedJoinRootFilterIsNotPushedIntoEverySystemTableLeaf()
  {
    final SystemTableDescriptor descriptor = new ServerPropertiesTableDescriptor();
    final List<ScanQuery> nodeQueries = captureNodeQueries(
        descriptor,
        selfJoin(descriptor),
        new SelectorDimFilter("value", "left", null)
    );

    Assertions.assertEquals(2, nodeQueries.size());
    Assertions.assertNull(nodeQueries.get(0).getFilter());
    Assertions.assertNull(nodeQueries.get(1).getFilter());
  }

  /**
   * Native query form of two filtered subqueries joined together:
   *
   * <pre>{@code
   * SELECT l.value
   * FROM (SELECT property, value FROM sys.server_properties WHERE value = 'left') AS l
   * JOIN (SELECT property, value FROM sys.server_properties WHERE value = 'right') AS r
   *   ON l.property = r.property
   * }</pre>
   *
   * Each subquery filter is owned by its own system table leaf and remains eligible for node pushdown.
   */
  @Test
  public void testQueryDataSourceFiltersArePushedOnlyIntoTheirOwningLeaves()
  {
    final SystemTableDescriptor descriptor = new ServerPropertiesTableDescriptor();
    final SelectorDimFilter leftFilter = new SelectorDimFilter("value", "left", null);
    final SelectorDimFilter rightFilter = new SelectorDimFilter("value", "right", null);
    final DataSource dataSource = JoinDataSource.create(
        new QueryDataSource(
            Druids.ScanQueryBuilder.copy(query(descriptor, Collections.emptyMap())).filters(leftFilter).build()
        ),
        new QueryDataSource(
            Druids.ScanQueryBuilder.copy(query(descriptor, Collections.emptyMap())).filters(rightFilter).build()
        ),
        "j.",
        "property == \"j.property\"",
        JoinType.INNER,
        null,
        ExprMacroTable.nil(),
        null,
        JoinAlgorithm.BROADCAST
    );

    final List<ScanQuery> nodeQueries = captureNodeQueries(descriptor, dataSource, null);

    Assertions.assertEquals(2, nodeQueries.size());
    Assertions.assertEquals(leftFilter, nodeQueries.get(0).getFilter());
    Assertions.assertEquals(rightFilter, nodeQueries.get(1).getFilter());
  }

  /** A filter pushed into a node scan carries the virtual column referenced by that filter. */
  @Test
  public void testPushedFilterCarriesVirtualColumns()
  {
    final SystemTableDescriptor descriptor = new ServerPropertiesTableDescriptor();
    final AtomicReference<ScanQuery> capturedNodeQuery = new AtomicReference<>();
    final ExpressionVirtualColumn virtualColumn = new ExpressionVirtualColumn(
        "upper_property",
        "upper(property)",
        ColumnType.STRING,
        ExprMacroTable.nil()
    );
    final SystemTableQueryClient client = makeClient(
        descriptor,
        List.of(testNode()),
        (queryPlus, responseContext) -> {
          capturedNodeQuery.set((ScanQuery) queryPlus.getQuery());
          return Sequences.empty();
        }
    );
    final ScanQuery query = Druids.newScanQueryBuilder()
                                 .dataSource(new SystemTableDataSource(descriptor.getTableName()))
                                 .eternityInterval()
                                 .virtualColumns(VirtualColumns.create(virtualColumn))
                                 .filters(new SelectorDimFilter("upper_property", "MATCH", null))
                                 .build();

    client.createRunner(query, AUTHENTICATION_RESULT, false)
          .run(QueryPlus.wrap(query), ResponseContext.createEmpty())
          .toList();

    Assertions.assertEquals(
        VirtualColumns.create(virtualColumn),
        capturedNodeQuery.get().getVirtualColumns()
    );
  }

  /** A window leaf filter also carries its leaf virtual columns into the node scan. */
  @Test
  public void testWindowLeafFilterCarriesVirtualColumns()
  {
    final SystemTableDescriptor descriptor = new ServerPropertiesTableDescriptor();
    final AtomicReference<ScanQuery> capturedNodeQuery = new AtomicReference<>();
    final ExpressionVirtualColumn virtualColumn = new ExpressionVirtualColumn(
        "upper_property",
        "upper(property)",
        ColumnType.STRING,
        ExprMacroTable.nil()
    );
    final SystemTableQueryClient client = makeClient(
        descriptor,
        List.of(testNode()),
        (queryPlus, responseContext) -> {
          capturedNodeQuery.set((ScanQuery) queryPlus.getQuery());
          return Sequences.empty();
        }
    );
    final WindowOperatorQuery query = new WindowOperatorQuery(
        new SystemTableDataSource(descriptor.getTableName()),
        new LegacySegmentSpec(Intervals.ETERNITY),
        Collections.emptyMap(),
        descriptor.getRowSignature(),
        Collections.emptyList(),
        List.of(
            new ScanOperatorFactory(
                null,
                new SelectorDimFilter("upper_property", "MATCH", null),
                null,
                null,
                VirtualColumns.create(virtualColumn),
                null
            )
        )
    );

    client.createRunner(query, AUTHENTICATION_RESULT, false)
          .run(QueryPlus.wrap(query), ResponseContext.createEmpty())
          .toList();

    Assertions.assertEquals(
        VirtualColumns.create(virtualColumn),
        capturedNodeQuery.get().getVirtualColumns()
    );
  }

  /** The private node transport always uses plain scan results even when the user query requests by-segment results. */
  @Test
  public void testNodeQueryDisablesBySegmentContext()
  {
    final SystemTableDescriptor descriptor = new ServerPropertiesTableDescriptor();
    final AtomicReference<ScanQuery> capturedNodeQuery = new AtomicReference<>();
    final SystemTableQueryClient client = makeClient(
        descriptor,
        List.of(testNode()),
        (queryPlus, responseContext) -> {
          capturedNodeQuery.set((ScanQuery) queryPlus.getQuery());
          return Sequences.empty();
        }
    );
    final ScanQuery query = query(descriptor, Map.of(QueryContexts.BY_SEGMENT_KEY, true));

    client.createRunner(query, AUTHENTICATION_RESULT, false)
          .run(QueryPlus.wrap(query), ResponseContext.createEmpty())
          .toList();

    Assertions.assertFalse(capturedNodeQuery.get().context().isBySegment());
  }

  /** A node query with {@code timeout = 0} retains Druid's no-timeout semantics. */
  @Test
  public void testNoTimeoutNodeQueryDoesNotGetImmediateDeadline()
  {
    final AtomicReference<ScanQuery> capturedNodeQuery = new AtomicReference<>();
    final SystemTableDescriptor descriptor = new TestSystemTableDescriptor();
    final SystemTableQueryClient client = makeClient(
        descriptor,
        List.of(testNode()),
        (queryPlus, responseContext) -> {
          capturedNodeQuery.set((ScanQuery) queryPlus.getQuery());
          return Sequences.empty();
        }
    );
    final ScanQuery query = query(
        descriptor,
        Map.of(
            QueryContexts.TIMEOUT_KEY,
            QueryContexts.NO_TIMEOUT,
            DirectDruidClient.QUERY_FAIL_TIME,
            0L
        )
    );

    client.createRunner(query, AUTHENTICATION_RESULT, false)
          .run(QueryPlus.wrap(query), ResponseContext.createEmpty())
          .toList();

    Assertions.assertEquals(
        JodaUtils.MAX_INSTANT,
        capturedNodeQuery.get().context().getLong(DirectDruidClient.QUERY_FAIL_TIME)
    );
  }

  /** Closing a partially consumed Broker result closes the initialized sequence and cancels every node query. */
  @Test
  public void testClosingBrokerResultClosesPartiallyConsumedNodeSequence() throws Exception
  {
    final AtomicInteger closeCalls = new AtomicInteger();
    final QueryScheduler queryScheduler = Mockito.mock(QueryScheduler.class);
    final SystemTableDescriptor descriptor = new TestSystemTableDescriptor();
    final SystemTableQueryClient client = makeClient(
        descriptor,
        List.of(testNode(8081), testNode(8082)),
        ignored -> (queryPlus, responseContext) -> Sequences.withBaggage(
            Sequences.simple(
                List.of(
                    scanResult(new Object[]{"first"}),
                    scanResult(new Object[]{"second"})
                )
            ),
            (Closeable) closeCalls::incrementAndGet
        ),
        passthroughRunner(descriptor.getRowSignature()),
        queryScheduler
    );
    final ScanQuery query = query(descriptor, Collections.emptyMap());

    final Sequence<ScanResultValue> brokerResults = client.createRunner(query, AUTHENTICATION_RESULT, false)
                                                           .run(
                                                               QueryPlus.wrap(query),
                                                               ResponseContext.createEmpty()
                                                           );
    final Yielder<ScanResultValue> yielder = Yielders.each(brokerResults);
    Assertions.assertFalse(yielder.isDone());

    yielder.close();

    Assertions.assertEquals(1, closeCalls.get());
    Mockito.verify(queryScheduler, Mockito.times(2)).cancelQuery(
        ArgumentMatchers.argThat(queryId -> queryId.startsWith(SystemTableDataSource.NODE_QUERY_ID_PREFIX))
    );
  }

  /** A node made unavailable by a network failure contributes an {@code error_message} row. */
  @Test
  public void testServerPropertiesNodeFailureBecomesErrorRow()
  {
    final ServerPropertiesTableDescriptor descriptor = new ServerPropertiesTableDescriptor();
    final DiscoveryDruidNode healthyNode = testNode(8081);
    final DiscoveryDruidNode failedNode = testNode(8082);
    final Object[] healthyRow = new Object[]{
        healthyNode.getDruidNode().getHostAndPortToUse(),
        healthyNode.getDruidNode().getServiceName(),
        "[broker]",
        "property",
        "value",
        null
    };
    final AtomicInteger nodeNumber = new AtomicInteger();
    final SystemTableQueryClient client = makeClient(
        descriptor,
        List.of(healthyNode, failedNode),
        ignored -> {
          if (nodeNumber.getAndIncrement() == 0) {
            return (queryPlus, responseContext) -> Sequences.simple(
                List.of(
                    new ScanResultValue(
                        null,
                        descriptor.getRowSignature().getColumnNames(),
                        List.of((Object) healthyRow)
                    )
                )
            );
          }
          return (queryPlus, responseContext) -> {
            throw new QueryInterruptedException(new ConnectException("node unavailable"));
          };
        }
    );
    final ScanQuery query = query(descriptor, Collections.emptyMap());

    final List<ScanResultValue> results = client.createRunner(query, AUTHENTICATION_RESULT, false)
                                                .run(QueryPlus.wrap(query), ResponseContext.createEmpty())
                                                .toList();

    Assertions.assertEquals(2, results.size());
    Assertions.assertArrayEquals(healthyRow, (Object[]) ((List<?>) results.get(0).getEvents()).get(0));
    Assertions.assertArrayEquals(
        new Object[]{
            failedNode.getDruidNode().getHostAndPortToUse(),
            failedNode.getDruidNode().getServiceName(),
            "[broker]",
            null,
            null,
            "node unavailable"
        },
        (Object[]) ((List<?>) results.get(1).getEvents()).get(0)
    );
  }

  /** No discovered nodes produce an empty {@code sys.server_properties} result, as in the Bindable path. */
  @Test
  public void testServerPropertiesWithNoDiscoveredNodesReturnsEmptyResult()
  {
    final ServerPropertiesTableDescriptor descriptor = new ServerPropertiesTableDescriptor();
    final SystemTableQueryClient client = makeClient(
        descriptor,
        Collections.emptyList(),
        (queryPlus, responseContext) -> Sequences.empty()
    );
    final ScanQuery query = query(descriptor, Collections.emptyMap());

    final List<ScanResultValue> results = client.createRunner(query, AUTHENTICATION_RESULT, false)
                                                .run(QueryPlus.wrap(query), ResponseContext.createEmpty())
                                                .toList();

    Assertions.assertTrue(results.isEmpty());
  }

  /** A node timeout terminates {@code sys.server_properties} instead of becoming an error row. */
  @Test
  public void testServerPropertiesTimeoutIsPropagated()
  {
    assertServerPropertiesQueryFailureIsPropagated(new QueryTimeoutException("node timed out"));
  }

  /** A node cancellation terminates {@code sys.server_properties} instead of becoming an error row. */
  @Test
  public void testServerPropertiesCancellationIsPropagated()
  {
    assertServerPropertiesQueryFailureIsPropagated(
        new QueryInterruptedException(
            QueryException.QUERY_CANCELED_ERROR_CODE,
            "node cancelled",
            QueryInterruptedException.class.getName(),
            "localhost"
        )
    );
  }

  /** An interrupted node request terminates {@code sys.server_properties} instead of becoming an error row. */
  @Test
  public void testServerPropertiesInterruptionIsPropagated()
  {
    assertServerPropertiesQueryFailureIsPropagated(new QueryInterruptedException(new InterruptedException()));
  }

  /** Authorization, capacity, resource-limit, unsupported-query, and unexpected failures terminate the query. */
  @Test
  public void testServerPropertiesNonAvailabilityFailuresArePropagated()
  {
    for (final RuntimeException failure : List.of(
        new ForbiddenException("forbidden"),
        new QueryCapacityExceededException(1),
        new ResourceLimitExceededException("resource limit"),
        new QueryUnsupportedException("unsupported"),
        new QueryInterruptedException(new IOException("malformed response")),
        new RuntimeException("unexpected")
    )) {
      assertServerPropertiesQueryFailureIsPropagated(failure);
    }
  }

  /** A timeout raised while reading a node response is also propagated. */
  @Test
  public void testServerPropertiesMidStreamTimeoutIsPropagated()
  {
    final QueryTimeoutException failure = new QueryTimeoutException("node timed out while streaming");
    final ServerPropertiesTableDescriptor descriptor = new ServerPropertiesTableDescriptor();
    final SystemTableQueryClient client = makeClient(
        descriptor,
        List.of(testNode()),
        (queryPlus, responseContext) -> Sequences.simple(List.of(scanResult(new Object[]{"ignored"})))
                                                    .map(result -> {
                                                      throw failure;
                                                    })
    );
    final ScanQuery query = query(descriptor, Collections.emptyMap());

    final QueryTimeoutException thrown = Assertions.assertThrows(
        QueryTimeoutException.class,
        () -> client.createRunner(query, AUTHENTICATION_RESULT, false)
                    .run(QueryPlus.wrap(query), ResponseContext.createEmpty())
                    .toList()
    );
    Assertions.assertSame(failure, thrown);
  }

  /** A leader-only node scan re-resolves the leader once when the selected node loses leadership before emitting rows. */
  @Test
  public void testLeaderOnlyQueryRetriesAfterLeadershipChange()
  {
    final SystemTableDescriptor descriptor = new TaskTableDescriptor();
    final SystemTableNode oldLeader = systemTableNode(testNode(8081));
    final SystemTableNode newLeader = systemTableNode(testNode(8082));
    final SystemTableNodeLocator nodeLocator = Mockito.mock(SystemTableNodeLocator.class);
    Mockito.when(nodeLocator.locate(Mockito.eq(descriptor), ArgumentMatchers.any()))
           .thenReturn(List.of(oldLeader), List.of(newLeader));
    final DirectDruidClientFactory directClientFactory = Mockito.mock(DirectDruidClientFactory.class);
    Mockito.when(directClientFactory.makeDirectClient(ArgumentMatchers.any())).thenAnswer(invocation -> {
      final DruidServer server = invocation.getArgument(0);
      @SuppressWarnings("unchecked")
      final DirectDruidClient<ScanResultValue> directClient = Mockito.mock(DirectDruidClient.class);
      if (server.getHost().endsWith(":8081")) {
        Mockito.when(directClient.run(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(
            Sequences.simple(List.of(scanResult(taskRow("stale")))).map(ignored -> {
              throw new QueryInterruptedException(new SystemTableNotLeaderException("overlord"));
            })
        );
      } else {
        Mockito.when(directClient.run(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(
            Sequences.simple(List.of(scanResult(taskRow("current"))))
        );
      }
      return directClient;
    });
    final QuerySegmentWalker querySegmentWalker = Mockito.mock(QuerySegmentWalker.class);
    Mockito.when(querySegmentWalker.getQueryRunnerForIntervals(ArgumentMatchers.any(), ArgumentMatchers.any()))
           .thenAnswer(ignored -> passthroughRunner(descriptor.getRowSignature()));
    final SystemTableQueryClient client = new SystemTableQueryClient(
        nodeLocator,
        directClientFactory,
        Mockito.mock(QueryScheduler.class),
        querySegmentWalker,
        Map.of(descriptor.getTableName(), descriptor),
        new AuthorizerMapper(Map.of("allow", new AllowAllAuthorizer(null))),
        Mockito.mock(SystemTableQueryHandler.class),
        NoopEscalator.getInstance(),
        nonMatchingSelfNode()
    );
    final ScanQuery query = query(descriptor, Collections.emptyMap());

    final List<ScanResultValue> results = client.createRunner(query, AUTHENTICATION_RESULT, false)
                                                .run(QueryPlus.wrap(query), ResponseContext.createEmpty())
                                                .toList();

    Assertions.assertEquals(1, results.size());
    Assertions.assertArrayEquals(
        taskRow("current"),
        (Object[]) ((List<?>) results.get(0).getEvents()).get(0)
    );
    Mockito.verify(nodeLocator, Mockito.times(2)).locate(Mockito.eq(descriptor), ArgumentMatchers.any());
  }

  private static void assertServerPropertiesQueryFailureIsPropagated(final RuntimeException failure)
  {
    final ServerPropertiesTableDescriptor descriptor = new ServerPropertiesTableDescriptor();
    final SystemTableQueryClient client = makeClient(
        descriptor,
        List.of(testNode()),
        (queryPlus, responseContext) -> {
          throw failure;
        }
    );
    final ScanQuery query = query(descriptor, Collections.emptyMap());

    final RuntimeException thrown = Assertions.assertThrows(
        failure.getClass(),
        () -> client.createRunner(query, AUTHENTICATION_RESULT, false)
                    .run(QueryPlus.wrap(query), ResponseContext.createEmpty())
                    .toList()
    );
    Assertions.assertSame(failure, thrown);
  }

  private static SystemTableQueryClient makeClient(
      final SystemTableDescriptor descriptor,
      final List<DiscoveryDruidNode> discoveryNodes,
      final QueryRunner<ScanResultValue> nodeRunner
  )
  {
    return makeClient(descriptor, discoveryNodes, ignored -> nodeRunner);
  }

  private static SystemTableQueryClient makeDirectClientBackedClient(
      final SystemTableDescriptor descriptor,
      final List<DiscoveryDruidNode> discoveryNodes,
      final ObjectMapper objectMapper,
      final HttpClient httpClient,
      final ScheduledExecutorService cancellationExecutor,
      final QueryScheduler queryScheduler
  )
  {
    final SystemTableNodeLocator nodeLocator = Mockito.mock(SystemTableNodeLocator.class);
    Mockito.when(nodeLocator.locate(Mockito.eq(descriptor), ArgumentMatchers.any())).thenReturn(
        discoveryNodes.stream().map(discoveryNode -> {
          final SystemTableNode systemTableNode = new SystemTableNode(discoveryNode);
          systemTableNode.addNodeRole(discoveryNode.getNodeRole());
          return systemTableNode;
        }).toList()
    );
    final DirectDruidClientFactory directClientFactory = Mockito.mock(DirectDruidClientFactory.class);
    Mockito.when(directClientFactory.makeDirectClient(ArgumentMatchers.any())).thenAnswer(
        invocation -> directClient(
            objectMapper,
            httpClient,
            ((DruidServer) invocation.getArgument(0)).getHost(),
            cancellationExecutor
        )
    );
    final QuerySegmentWalker querySegmentWalker = Mockito.mock(QuerySegmentWalker.class);
    Mockito.when(querySegmentWalker.getQueryRunnerForIntervals(ArgumentMatchers.any(), ArgumentMatchers.any()))
           .thenAnswer(ignored -> passthroughRunner(descriptor.getRowSignature()));
    return new SystemTableQueryClient(
        nodeLocator,
        directClientFactory,
        queryScheduler,
        querySegmentWalker,
        Map.of(descriptor.getTableName(), descriptor),
        new AuthorizerMapper(Map.of("allow", new AllowAllAuthorizer(null))),
        Mockito.mock(SystemTableQueryHandler.class),
        NoopEscalator.getInstance(),
        nonMatchingSelfNode()
    );
  }

  private static SystemTableQueryClient makeClient(
      final SystemTableDescriptor descriptor,
      final List<DiscoveryDruidNode> discoveryNodes,
      final Function<DruidServer, QueryRunner<ScanResultValue>> nodeRunnerFactory
  )
  {
    return makeClient(
        descriptor,
        discoveryNodes,
        nodeRunnerFactory,
        passthroughRunner(descriptor.getRowSignature())
    );
  }

  private static SystemTableQueryClient makeClient(
      final SystemTableDescriptor descriptor,
      final List<DiscoveryDruidNode> discoveryNodes,
      final Function<DruidServer, QueryRunner<ScanResultValue>> nodeRunnerFactory,
      final QueryRunner<?> queryRunner
  )
  {
    return makeClient(
        descriptor,
        discoveryNodes,
        nodeRunnerFactory,
        queryRunner,
        Mockito.mock(QueryScheduler.class)
    );
  }

  private static SystemTableQueryClient makeClient(
      final SystemTableDescriptor descriptor,
      final List<DiscoveryDruidNode> discoveryNodes,
      final Function<DruidServer, QueryRunner<ScanResultValue>> nodeRunnerFactory,
      final QueryRunner<?> queryRunner,
      final QueryScheduler queryScheduler
  )
  {
    final SystemTableNodeLocator nodeLocator = Mockito.mock(SystemTableNodeLocator.class);
    Mockito.when(nodeLocator.locate(Mockito.eq(descriptor), ArgumentMatchers.any())).thenReturn(
        discoveryNodes.stream().map(discoveryNode -> {
          final SystemTableNode systemTableNode = new SystemTableNode(discoveryNode);
          systemTableNode.addNodeRole(discoveryNode.getNodeRole());
          return systemTableNode;
        }).toList()
    );

    final DirectDruidClientFactory directClientFactory = Mockito.mock(DirectDruidClientFactory.class);
    Mockito.when(directClientFactory.makeDirectClient(ArgumentMatchers.any())).thenAnswer(invocation -> {
      final QueryRunner<ScanResultValue> nodeRunner = nodeRunnerFactory.apply(invocation.getArgument(0));
      @SuppressWarnings("unchecked")
      final DirectDruidClient<ScanResultValue> directClient = Mockito.mock(DirectDruidClient.class);
      Mockito.when(directClient.run(ArgumentMatchers.any(), ArgumentMatchers.any())).thenAnswer(
          runInvocation -> nodeRunner.run(
              runInvocation.getArgument(0),
              runInvocation.getArgument(1)
          )
      );
      return directClient;
    });

    final QuerySegmentWalker querySegmentWalker = Mockito.mock(QuerySegmentWalker.class);
    Mockito.when(querySegmentWalker.getQueryRunnerForIntervals(ArgumentMatchers.any(), ArgumentMatchers.any()))
           .thenAnswer(invocation -> queryRunner);

    return new SystemTableQueryClient(
        nodeLocator,
        directClientFactory,
        queryScheduler,
        querySegmentWalker,
        Map.of(descriptor.getTableName(), descriptor),
        new AuthorizerMapper(Map.of("allow", new AllowAllAuthorizer(null))),
        Mockito.mock(SystemTableQueryHandler.class),
        NoopEscalator.getInstance(),
        nonMatchingSelfNode()
    );
  }

  private static DruidNode nonMatchingSelfNode()
  {
    return new DruidNode("broker-service", "localhost", false, 9082, null, true, false);
  }

  private static DataSource selfJoin(final SystemTableDescriptor descriptor)
  {
    return JoinDataSource.create(
        new SystemTableDataSource(descriptor.getTableName()),
        new SystemTableDataSource(descriptor.getTableName()),
        "j.",
        "property == \"j.property\"",
        JoinType.INNER,
        null,
        ExprMacroTable.nil(),
        null,
        JoinAlgorithm.BROADCAST
    );
  }

  private static List<ScanQuery> captureNodeQueries(
      final SystemTableDescriptor descriptor,
      final DataSource dataSource,
      @Nullable final DimFilter filter
  )
  {
    final List<ScanQuery> nodeQueries = new ArrayList<>();
    final SystemTableNodeLocator nodeLocator = Mockito.mock(SystemTableNodeLocator.class);
    final DiscoveryDruidNode discoveryNode = testNode();
    final SystemTableNode systemTableNode = new SystemTableNode(discoveryNode);
    systemTableNode.addNodeRole(discoveryNode.getNodeRole());
    Mockito.when(nodeLocator.locate(Mockito.eq(descriptor), ArgumentMatchers.any())).thenAnswer(invocation -> {
      nodeQueries.add(invocation.getArgument(1));
      return List.of(systemTableNode);
    });
    final DirectDruidClientFactory directClientFactory = Mockito.mock(DirectDruidClientFactory.class);
    Mockito.when(directClientFactory.makeDirectClient(ArgumentMatchers.any()))
           .thenReturn(Mockito.mock(DirectDruidClient.class));
    final QuerySegmentWalker querySegmentWalker = Mockito.mock(QuerySegmentWalker.class);
    Mockito.when(querySegmentWalker.getQueryRunnerForIntervals(ArgumentMatchers.any(), ArgumentMatchers.any()))
           .thenReturn((queryPlus, responseContext) -> Sequences.empty());
    final SystemTableQueryClient client = new SystemTableQueryClient(
        nodeLocator,
        directClientFactory,
        Mockito.mock(QueryScheduler.class),
        querySegmentWalker,
        Map.of(descriptor.getTableName(), descriptor),
        new AuthorizerMapper(Map.of("allow", new AllowAllAuthorizer(null))),
        Mockito.mock(SystemTableQueryHandler.class),
        NoopEscalator.getInstance(),
        nonMatchingSelfNode()
    );
    final ScanQuery query = Druids.newScanQueryBuilder()
                                 .dataSource(dataSource)
                                 .eternityInterval()
                                 .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                 .filters(filter)
                                 .build();

    client.createRunner(query, AUTHENTICATION_RESULT, false)
          .run(QueryPlus.wrap(query), ResponseContext.createEmpty())
          .toList();
    return nodeQueries;
  }

  private static void assertWrappedSystemTableIsResolved(
      final SystemTableDescriptor descriptor,
      final DataSource wrappedDataSource
  )
  {
    final AtomicReference<DataSource> resolvedDataSource = new AtomicReference<>();
    final QueryRunner<ScanResultValue> resolvedQueryRunner = (queryPlus, responseContext) -> {
      resolvedDataSource.set(queryPlus.getQuery().getDataSource());
      return Sequences.empty();
    };
    final SystemTableQueryClient client = makeClient(
        descriptor,
        List.of(testNode()),
        ignored -> (queryPlus, responseContext) -> Sequences.simple(List.of(scanResult(new Object[]{"value"}))),
        resolvedQueryRunner
    );
    final ScanQuery query = Druids.newScanQueryBuilder()
                                 .dataSource(wrappedDataSource)
                                 .eternityInterval()
                                 .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                 .build();

    client.createRunner(query, AUTHENTICATION_RESULT, false)
          .run(QueryPlus.wrap(query), ResponseContext.createEmpty())
          .toList();

    Assertions.assertNotNull(resolvedDataSource.get());
    Assertions.assertFalse(containsSystemTableDataSource(resolvedDataSource.get()));
  }

  private static boolean containsSystemTableDataSource(final DataSource dataSource)
  {
    if (dataSource instanceof SystemTableDataSource) {
      return true;
    }
    return dataSource.getChildren().stream().anyMatch(SystemTableQueryClientTest::containsSystemTableDataSource);
  }

  private static QueryRunner<ScanResultValue> passthroughRunner(final RowSignature rowSignature)
  {
    return (queryPlus, responseContext) -> {
      final InlineDataSource dataSource = (InlineDataSource) queryPlus.getQuery().getDataSource();
      return Sequences.simple(dataSource.getRows())
                      .map(row -> new ScanResultValue(null, rowSignature.getColumnNames(), List.of((Object) row)));
    };
  }

  private static ScanQuery query(final SystemTableDescriptor descriptor, final Map<String, Object> context)
  {
    return Druids.newScanQueryBuilder()
                 .dataSource(new SystemTableDataSource(descriptor.getTableName()))
                 .eternityInterval()
                 .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                 .context(context)
                 .build();
  }

  private static ScanResultValue scanResult(final Object[]... rows)
  {
    return new ScanResultValue(null, List.of("value"), List.of(rows));
  }

  private static Object[] taskRow(final String taskId)
  {
    return new Object[]{taskId, null, null, null, null, null, null, null, 0L, null, null, -1L, -1L, null};
  }

  private static DirectDruidClient<ScanResultValue> directClient(
      final ObjectMapper objectMapper,
      final HttpClient httpClient,
      final String host,
      final ScheduledExecutorService cancellationExecutor
  )
  {
    return new DirectDruidClient<>(
        CONGLOMERATE.getConglomerate(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        objectMapper,
        httpClient,
        "http",
        host,
        new NoopServiceEmitter(),
        cancellationExecutor
    );
  }

  private static class DelayedHttpClient implements HttpClient
  {
    private final List<SettableFuture<java.io.InputStream>> responses;
    private final CountDownLatch requestLatch;
    private final AtomicInteger requestNumber = new AtomicInteger();

    private DelayedHttpClient(final int requestCount)
    {
      responses = new ArrayList<>(requestCount);
      for (int i = 0; i < requestCount; i++) {
        responses.add(SettableFuture.create());
      }
      requestLatch = new CountDownLatch(requestCount);
    }

    private boolean awaitRequests() throws InterruptedException
    {
      return requestLatch.await(10, TimeUnit.SECONDS);
    }

    private CloseTrackingInputStream respond(final int request, final byte[] response)
    {
      final CloseTrackingInputStream inputStream = new CloseTrackingInputStream(response);
      responses.get(request).set(inputStream);
      return inputStream;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <Intermediate, Final> com.google.common.util.concurrent.ListenableFuture<Final> go(
        final Request request,
        final HttpResponseHandler<Intermediate, Final> handler,
        final Duration readTimeout
    )
    {
      final SettableFuture<java.io.InputStream> response = responses.get(requestNumber.getAndIncrement());
      requestLatch.countDown();
      return (com.google.common.util.concurrent.ListenableFuture<Final>) response;
    }

    @Override
    public <Intermediate, Final> com.google.common.util.concurrent.ListenableFuture<Final> go(
        final Request request,
        final HttpResponseHandler<Intermediate, Final> handler
    )
    {
      throw new UnsupportedOperationException();
    }
  }

  private static class CloseTrackingInputStream extends ByteArrayInputStream
  {
    private final AtomicBoolean closed = new AtomicBoolean();

    private CloseTrackingInputStream(final byte[] response)
    {
      super(response);
    }

    private boolean isClosed()
    {
      return closed.get();
    }

    @Override
    public void close() throws IOException
    {
      closed.set(true);
      super.close();
    }
  }

  private static DiscoveryDruidNode testNode()
  {
    return testNode(8082);
  }

  private static DiscoveryDruidNode testNode(final int port)
  {
    return new DiscoveryDruidNode(
        new DruidNode("broker-service", "localhost", false, port, null, true, false),
        NodeRole.BROKER,
        Collections.emptyMap()
    );
  }

  private static SystemTableNode systemTableNode(final DiscoveryDruidNode discoveryNode)
  {
    final SystemTableNode node = new SystemTableNode(discoveryNode);
    node.addNodeRole(discoveryNode.getNodeRole());
    return node;
  }

  private static class TestSystemTableDescriptor implements SystemTableDescriptor
  {
    private static final RowSignature ROW_SIGNATURE = RowSignature.builder().add("value", null).build();

    @Override
    public String getTableName()
    {
      return "test";
    }

    @Override
    public Set<NodeRole> getNodeRoles()
    {
      return Set.of(NodeRole.BROKER);
    }

    @Override
    public RowSignature getRowSignature()
    {
      return ROW_SIGNATURE;
    }

    @Override
    public org.apache.druid.server.system.table.SystemTableRowAuthorizer getRowAuthorizer()
    {
      return (rows, authenticationResult, authorizerMapper) -> rows;
    }
  }
}
