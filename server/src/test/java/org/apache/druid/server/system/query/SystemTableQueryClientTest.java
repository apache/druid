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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.client.DirectDruidClientFactory;
import org.apache.druid.client.DruidServer;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.JodaUtils;
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
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.SystemTableAccess;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.union.UnionQuery;
import org.apache.druid.segment.SegmentWrangler;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.system.table.ServerPropertiesTableDescriptor;
import org.apache.druid.server.system.table.SystemTableDataProvider;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.apache.druid.server.system.table.SystemTableSegmentWrangler;
import org.joda.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collections;
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
import java.util.stream.Stream;

public class SystemTableQueryClientTest
{
  @RegisterExtension
  public static final QueryStackTests.ConglomerateExtension CONGLOMERATE =
      new QueryStackTests.ConglomerateExtension();

  /**
   * Node responses arrive as Scan result batches, in list or compacted-list form, and are flattened into rows.
   */
  @Test
  public void testNodeRowsFlattenScanResultBatches()
  {
    final ScanQuery nodeQuery = Druids.newScanQueryBuilder()
                                            .dataSource(new SystemTableDataSource("test"))
                                            .eternityInterval()
                                            .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                            .build();
    final QueryRunner<ScanResultValue> nodeRunner = (queryPlus, responseContext) -> {
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

    final List<Object[]> flattenedRows = new ArrayList<>();
    rows.forEach(flattenedRows::add);
    Assertions.assertEquals(3, flattenedRows.size());
    Assertions.assertArrayEquals(new Object[]{"task-a"}, flattenedRows.get(0));
    Assertions.assertArrayEquals(new Object[]{"task-b"}, flattenedRows.get(1));
    Assertions.assertArrayEquals(new Object[]{"task-c"}, flattenedRows.get(2));
  }

  /**
   * The Broker reads its own rows of the table without an HTTP request, through the system-table segment.
   */
  @Test
  public void testBrokerNodeExecutesLocallyWithoutHttp()
  {
    final SystemTableDescriptor descriptor = new TestSystemTableDescriptor();
    final DirectDruidClientFactory directClientFactory = Mockito.mock(DirectDruidClientFactory.class);
    final TestClient client = makeBrokerLocalClient(descriptor, directClientFactory);
    final ScanQuery query = query(descriptor, Collections.emptyMap());

    final List<ScanResultValue> results = client.createRunner(query)
                                                .run(QueryPlus.wrap(query), ResponseContext.createEmpty())
                                                .toList();

    Assertions.assertEquals(1, results.size());
    final List<?> events = (List<?>) results.get(0).getEvents();
    Assertions.assertEquals(1, events.size());
    Assertions.assertArrayEquals(new Object[]{"local"}, (Object[]) events.get(0));
    Mockito.verifyNoInteractions(directClientFactory);
  }

  /**
   * The Broker's scan of its own rows stops at the query's deadline, like the scans sent to other services.
   */
  @Test
  public void testBrokerNodeScanHonorsQueryDeadline()
  {
    final SystemTableDescriptor descriptor = new TestSystemTableDescriptor();
    final TestClient client = makeBrokerLocalClient(descriptor, Mockito.mock(DirectDruidClientFactory.class));
    final ScanQuery query = query(
        descriptor,
        QueryContext.ofMap(QueryContextParameters.QUERY_FAIL_TIME, System.currentTimeMillis() - 1_000L)
    );

    Assertions.assertThrows(QueryTimeoutException.class, () -> runToList(client, query));
  }

  private static TestClient makeBrokerLocalClient(
      final SystemTableDescriptor descriptor,
      final DirectDruidClientFactory directClientFactory
  )
  {
    final DiscoveryDruidNode brokerNode = testNode();
    final SystemTableNodeLocator nodeLocator = Mockito.mock(SystemTableNodeLocator.class);
    final SystemTableNode node = new SystemTableNode(brokerNode);
    node.addNodeRole(brokerNode.getNodeRole());
    Mockito.when(nodeLocator.locate(Mockito.eq(descriptor))).thenReturn(List.of(node));

    final SystemTableDataProvider provider = filters -> List.<Object[]>of(new Object[]{"local"});
    final SystemTableSegmentWrangler segmentWrangler = new SystemTableSegmentWrangler(
        Map.of(descriptor.getTableName(), provider),
        Map.of(descriptor.getTableName(), descriptor)
    );
    return new TestClient(
        new SystemTableQueryClient(
            nodeLocator,
            directClientFactory,
            Mockito.mock(QueryScheduler.class),
            segmentWrangler,
            CONGLOMERATE.getConglomerate(),
            Map.of(descriptor.getTableName(), descriptor),
            new ServerConfig(),
            brokerNode.getDruidNode()
        ),
        passthroughRunner(descriptor.getRowSignature())
    );
  }


  /**
   * Nodes are queried with the escalated identity and return every row, so the rows are filtered on the Broker with the
   * caller's access, which the Broker attached when it authorized the query.
   */
  @Test
  public void testCallerAccessFiltersGatheredRows()
  {
    final SystemTableDescriptor descriptor = new TestSystemTableDescriptor();
    final TestClient client = makeClient(
        descriptor,
        List.of(testNode()),
        (queryPlus, responseContext) -> Sequences.simple(
            List.of(scanResult(new Object[]{"allowed"}, new Object[]{"secret"}))
        )
    );
    final ScanQuery query = query(descriptor, Collections.emptyMap());

    final SystemTableAccess access = new SystemTableAccess(row -> "allowed".equals(row[0]));
    final List<ScanResultValue> results = client.createRunner(query, access)
                                                .run(QueryPlus.wrap(query), ResponseContext.createEmpty())
                                                .toList();

    Assertions.assertEquals(1, results.size());
    Assertions.assertArrayEquals(new Object[]{"allowed"}, (Object[]) ((List<?>) results.get(0).getEvents()).get(0));
  }

  /**
   * A system table that wasn't authorized on this Broker can't be gathered, and no node is queried.
   */
  @Test
  public void testUnauthorizedSystemTableIsNotGathered()
  {
    final ServerPropertiesTableDescriptor descriptor = new ServerPropertiesTableDescriptor();
    final SystemTableNodeLocator nodeLocator = Mockito.mock(SystemTableNodeLocator.class);
    final DirectDruidClientFactory directClientFactory = Mockito.mock(DirectDruidClientFactory.class);
    final SystemTableQueryClient client = newClient(
        nodeLocator,
        directClientFactory,
        Mockito.mock(QueryScheduler.class),
        Map.of(descriptor.getTableName(), descriptor),
        nonMatchingSelfNode()
    );
    final ScanQuery query = Druids.ScanQueryBuilder
        .copy(query(descriptor, Collections.emptyMap()))
        .dataSource(new SystemTableDataSource(descriptor.getTableName()).withScope(SystemTableDataSource.Scope.CLUSTER))
        .build();

    final DruidException e = Assertions.assertThrows(DruidException.class, () -> client.resolve(query));
    Assertions.assertEquals(DruidException.Category.DEFENSIVE, e.getCategory());
    Mockito.verifyNoInteractions(nodeLocator, directClientFactory);
  }

  /**
   * Node scans must not share the caller's response context. A Scan with a limit returns no rows once the context's
   * scanned-row count reaches the limit, so counting node rows there emptied limited Scans over joins.
   */
  @Test
  public void testNodeScansDoNotShareCallerResponseContext()
  {
    final SystemTableDescriptor descriptor = new TestSystemTableDescriptor();
    final ResponseContext callerResponseContext = ResponseContext.createEmpty();
    callerResponseContext.initializeRowScanCount();
    final List<ResponseContext> nodeResponseContexts = new ArrayList<>();
    final TestClient client = makeClient(
        descriptor,
        List.of(testNode()),
        (queryPlus, responseContext) -> {
          nodeResponseContexts.add(responseContext);
          responseContext.addRowScanCount(100);
          return Sequences.simple(List.of(scanResult(new Object[]{"remote"})));
        }
    );
    final ScanQuery query = query(descriptor, Collections.emptyMap());

    final List<ScanResultValue> results = client.createRunner(query)
                                                .run(QueryPlus.wrap(query), callerResponseContext)
                                                .toList();

    Assertions.assertEquals(1, results.size());
    Assertions.assertEquals(1, nodeResponseContexts.size());
    Assertions.assertNotSame(callerResponseContext, nodeResponseContexts.get(0));
    Assertions.assertEquals(0L, callerResponseContext.getRowScanCount());
  }

  /**
   * Delayed node responses verify concurrent fanout through the real {@link DirectDruidClient} transport path.
   */
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
                                             .context(
                                                 QueryContext.ofMap(QueryContextParameters.QUERY_FAIL_TIME, Long.MAX_VALUE)
                                             )
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

  /**
   * Closing the result of a query that gathered rows closes the node responses and cancels every node query.
   */
  @Test
  public void testClosingResultCancelsNodeQueries() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final DelayedHttpClient httpClient = new DelayedHttpClient(2);
    final ScheduledExecutorService cancellationExecutor = Executors.newSingleThreadScheduledExecutor();
    final ExecutorService consumerExecutor = Executors.newSingleThreadExecutor();
    final QueryScheduler queryScheduler = Mockito.mock(QueryScheduler.class);
    final SystemTableDescriptor descriptor = new TestSystemTableDescriptor();
    try {
      final TestClient client = makeDirectClientBackedClient(
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
              client.createRunner(query)
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
   * A system table read by a subquery is gathered too.
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
   * A system table wrapped in a filtered datasource, as the DECOUPLED planner plans filtered window queries, is
   * gathered too.
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
   * WHERE r.value = 'match'   -- or the unprefixed l.value = 'match'
   * }</pre>
   *
   * A root filter belongs to the join result, whether or not it uses the right-side join prefix. It must remain on
   * the Broker instead of being copied into either node-local system table scan.
   */
  @ParameterizedTest
  @ValueSource(strings = {"j.value", "value"})
  public void testJoinRootFilterIsNotPushedIntoSystemTableLeaves(final String filterColumn)
  {
    final SystemTableDescriptor descriptor = new ServerPropertiesTableDescriptor();
    final List<ScanQuery> nodeQueries = captureNodeQueries(
        descriptor,
        selfJoin(descriptor),
        new SelectorDimFilter(filterColumn, "match", null)
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

  /**
   * Each query of a union gathers its own system table, with its own filter pushed into the node scans.
   */
  @Test
  public void testUnionQueriesGatherTheirOwnSystemTables()
  {
    final SystemTableDescriptor descriptor = new ServerPropertiesTableDescriptor();
    final SelectorDimFilter filter = new SelectorDimFilter("server", "localhost:8082", null);
    final DataSource dataSource = new QueryDataSource(
        new UnionQuery(
            List.of(
                Druids.ScanQueryBuilder.copy(query(descriptor, Collections.emptyMap())).filters(filter).build(),
                query(descriptor, Collections.emptyMap())
            )
        )
    );

    final List<ScanQuery> nodeQueries = captureNodeQueries(descriptor, dataSource, null);

    Assertions.assertEquals(2, nodeQueries.size());
    Assertions.assertEquals(filter, nodeQueries.get(0).getFilter());
    Assertions.assertNull(nodeQueries.get(1).getFilter());
  }

  /**
   * A query of a union keeps its own deadline when it's tighter than the union's, and its node scans use it.
   */
  @Test
  public void testUnionQueryKeepsItsTighterDeadline()
  {
    final SystemTableDescriptor descriptor = new ServerPropertiesTableDescriptor();
    final long armDeadline = System.currentTimeMillis() + 60_000L;
    final DataSource dataSource = new QueryDataSource(
        new UnionQuery(
            List.of(
                query(descriptor, Collections.emptyMap()),
                query(descriptor, QueryContext.ofMap(QueryContextParameters.QUERY_FAIL_TIME, armDeadline))
            )
        )
    );

    final List<ScanQuery> nodeQueries = captureNodeQueries(descriptor, dataSource, null);

    Assertions.assertEquals(2, nodeQueries.size());
    Assertions.assertTrue(
        nodeQueries.get(0).context().get(QueryContextParameters.QUERY_FAIL_TIME) > armDeadline
    );
    Assertions.assertEquals(armDeadline, nodeQueries.get(1).context().get(QueryContextParameters.QUERY_FAIL_TIME));
  }

  /**
   * Only conjuncts that read columns of the table are pushed into node scans, and virtual columns never are. The
   * resolved query still applies its whole filter to the gathered rows.
   */
  @Test
  public void testFilterOnVirtualColumnIsNotPushed()
  {
    final SystemTableDescriptor descriptor = new ServerPropertiesTableDescriptor();
    final ExpressionVirtualColumn virtualColumn = new ExpressionVirtualColumn(
        "upper_property",
        "upper(property)",
        ColumnType.STRING,
        ExprMacroTable.nil()
    );
    final SelectorDimFilter tableFilter = new SelectorDimFilter("service_name", "druid/broker", null);
    final ScanQuery nodeQuery = captureNodeQuery(
        descriptor,
        Druids.newScanQueryBuilder()
              .dataSource(new SystemTableDataSource(descriptor.getTableName()))
              .eternityInterval()
              .virtualColumns(VirtualColumns.create(virtualColumn))
              .filters(new AndDimFilter(new SelectorDimFilter("upper_property", "MATCH", null), tableFilter))
              .build()
    );

    Assertions.assertEquals(tableFilter, nodeQuery.getFilter());
    Assertions.assertEquals(VirtualColumns.EMPTY, nodeQuery.getVirtualColumns());
  }

  /**
   * A virtual column that replaces a column of the table must not reach the node scans: the gathered rows must hold the
   * table's own values, which are authorized on the Broker after they are gathered. Filters on such a column aren't
   * pushed down either.
   */
  @Test
  public void testVirtualColumnShadowingTableColumnIsNotPushed()
  {
    final SystemTableDescriptor descriptor = new ServerPropertiesTableDescriptor();
    final ScanQuery nodeQuery = captureNodeQuery(
        descriptor,
        Druids.newScanQueryBuilder()
              .dataSource(new SystemTableDataSource(descriptor.getTableName()))
              .eternityInterval()
              .virtualColumns(
                  VirtualColumns.create(
                      new ExpressionVirtualColumn("server", "'spoofed'", ColumnType.STRING, ExprMacroTable.nil())
                  )
              )
              .filters(new SelectorDimFilter("server", "spoofed", null))
              .build()
    );

    Assertions.assertNull(nodeQuery.getFilter());
    Assertions.assertEquals(VirtualColumns.EMPTY, nodeQuery.getVirtualColumns());
    Assertions.assertEquals(descriptor.getRowSignature().getColumnNames(), nodeQuery.getColumns());
  }

  /**
   * The private node transport always uses plain scan results even when the user query requests by-segment results.
   */
  @Test
  public void testNodeQueryDisablesBySegmentContext()
  {
    final SystemTableDescriptor descriptor = new ServerPropertiesTableDescriptor();
    final AtomicReference<ScanQuery> capturedNodeQuery = new AtomicReference<>();
    final TestClient client = makeClient(
        descriptor,
        List.of(testNode()),
        (queryPlus, responseContext) -> {
          capturedNodeQuery.set((ScanQuery) queryPlus.getQuery());
          return Sequences.empty();
        }
    );
    final ScanQuery query = query(descriptor, QueryContext.ofMap(QueryContextParameters.BY_SEGMENT, true));

    client.createRunner(query)
          .run(QueryPlus.wrap(query), ResponseContext.createEmpty())
          .toList();

    Assertions.assertFalse(capturedNodeQuery.get().context().isBySegment());
  }

  /**
   * SELECT * FROM (SELECT * FROM sys.server_properties) must retain the outer query deadline.
   */
  @ParameterizedTest
  @ValueSource(longs = {0L, 7_000L, Long.MAX_VALUE})
  public void testSubqueryRetainsOuterDeadline(final long innerTimeout)
  {
    final AtomicReference<ScanQuery> captured = new AtomicReference<>();
    final SystemTableDescriptor descriptor = new TestSystemTableDescriptor();
    final TestClient client = makeClient(
        descriptor,
        List.of(testNode()),
        ignored -> (queryPlus, responseContext) -> {
          captured.set((ScanQuery) queryPlus.getQuery());
          return Sequences.empty();
        },
        (queryPlus, responseContext) -> {
          final QueryDataSource subquery = (QueryDataSource) queryPlus.getQuery().getDataSource();
          final InlineDataSource inline = (InlineDataSource) subquery.getQuery().getDataSource();
          Assertions.assertFalse(inline.getRows().iterator().hasNext());
          return Sequences.empty();
        }
    );
    final long deadline = System.currentTimeMillis() + 1_000L;
    final ScanQuery inner = query(descriptor, QueryContext.ofMap(QueryContextParameters.TIMEOUT, innerTimeout));
    final ScanQuery outer = query(
        descriptor,
        QueryContext.ofMap(
            QueryContextParameters.QUERY_FAIL_TIME,
            deadline,
            QueryContextParameters.MAX_SCATTER_GATHER_BYTES,
            1_234L
        )
    )
        .withDataSource(new QueryDataSource(inner));

    client.createRunner(outer)
          .run(QueryPlus.wrap(outer), ResponseContext.createEmpty())
          .toList();

    Assertions.assertEquals(deadline, captured.get().context().get(QueryContextParameters.QUERY_FAIL_TIME));
    Assertions.assertEquals(1_234L, captured.get().context().getMaxScatterGatherBytes());
  }

  /**
   * A node query with {@code timeout = 0} retains Druid's no-timeout semantics.
   */
  @Test
  public void testNoTimeoutNodeQueryDoesNotGetImmediateDeadline()
  {
    final AtomicReference<ScanQuery> capturedNodeQuery = new AtomicReference<>();
    final SystemTableDescriptor descriptor = new TestSystemTableDescriptor();
    final TestClient client = makeClient(
        descriptor,
        List.of(testNode()),
        (queryPlus, responseContext) -> {
          capturedNodeQuery.set((ScanQuery) queryPlus.getQuery());
          return Sequences.empty();
        }
    );
    final ScanQuery query = query(
        descriptor,
        QueryContext.ofMap(
            QueryContextParameters.TIMEOUT,
            QueryContexts.NO_TIMEOUT,
            QueryContextParameters.QUERY_FAIL_TIME,
            0L
        )
    );

    client.createRunner(query)
          .run(QueryPlus.wrap(query), ResponseContext.createEmpty())
          .toList();

    Assertions.assertEquals(
        JodaUtils.MAX_INSTANT,
        capturedNodeQuery.get().context().get(QueryContextParameters.QUERY_FAIL_TIME)
    );
  }

  /**
   * A node made unavailable by a network failure contributes an {@code error_message} row.
   */
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
    final TestClient client = makeClient(
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

    final List<ScanResultValue> results = client.createRunner(query)
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

  /**
   * No discovered nodes produce an empty {@code sys.server_properties} result, as in the Bindable path.
   */
  @Test
  public void testServerPropertiesWithNoDiscoveredNodesReturnsEmptyResult()
  {
    final ServerPropertiesTableDescriptor descriptor = new ServerPropertiesTableDescriptor();
    final TestClient client = makeClient(
        descriptor,
        Collections.emptyList(),
        (queryPlus, responseContext) -> Sequences.empty()
    );
    final ScanQuery query = query(descriptor, Collections.emptyMap());

    final List<ScanResultValue> results = client.createRunner(query)
                                                .run(QueryPlus.wrap(query), ResponseContext.createEmpty())
                                                .toList();

    Assertions.assertTrue(results.isEmpty());
  }

  /**
   * Timeouts, cancellations, interruptions, and other failures that do not indicate an unreachable node terminate
   * {@code sys.server_properties} instead of becoming an error row.
   */
  @ParameterizedTest
  @MethodSource("nonAvailabilityFailures")
  public void testServerPropertiesNonAvailabilityFailureIsPropagated(final RuntimeException failure)
  {
    assertServerPropertiesQueryFailureIsPropagated(failure);
  }

  private static Stream<RuntimeException> nonAvailabilityFailures()
  {
    // Only failures to reach a node become failure rows. These cover a query error, an I/O error that isn't a
    // connection failure, and an unexpected error.
    return Stream.of(
        new QueryTimeoutException("node timed out"),
        new QueryInterruptedException(new IOException("malformed response")),
        new RuntimeException("unexpected")
    );
  }

  /**
   * A timeout raised while reading a node response is also propagated.
   */
  @Test
  public void testServerPropertiesMidStreamTimeoutIsPropagated()
  {
    final QueryTimeoutException failure = new QueryTimeoutException("node timed out while streaming");
    final ServerPropertiesTableDescriptor descriptor = new ServerPropertiesTableDescriptor();
    final TestClient client = makeClient(
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
        () -> client.createRunner(query)
                    .run(QueryPlus.wrap(query), ResponseContext.createEmpty())
                    .toList()
    );
    Assertions.assertSame(failure, thrown);
  }

  private static void assertServerPropertiesQueryFailureIsPropagated(final RuntimeException failure)
  {
    final ServerPropertiesTableDescriptor descriptor = new ServerPropertiesTableDescriptor();
    final TestClient client = makeClient(
        descriptor,
        List.of(testNode()),
        (queryPlus, responseContext) -> {
          throw failure;
        }
    );
    final ScanQuery query = query(descriptor, Collections.emptyMap());

    final RuntimeException thrown = Assertions.assertThrows(
        failure.getClass(),
        () -> client.createRunner(query)
                    .run(QueryPlus.wrap(query), ResponseContext.createEmpty())
                    .toList()
    );
    Assertions.assertSame(failure, thrown);
  }

  private static TestClient makeClient(
      final SystemTableDescriptor descriptor,
      final List<DiscoveryDruidNode> discoveryNodes,
      final QueryRunner<ScanResultValue> nodeRunner
  )
  {
    return makeClient(descriptor, discoveryNodes, ignored -> nodeRunner);
  }

  private static TestClient makeDirectClientBackedClient(
      final SystemTableDescriptor descriptor,
      final List<DiscoveryDruidNode> discoveryNodes,
      final ObjectMapper objectMapper,
      final HttpClient httpClient,
      final ScheduledExecutorService cancellationExecutor,
      final QueryScheduler queryScheduler
  )
  {
    final SystemTableNodeLocator nodeLocator = Mockito.mock(SystemTableNodeLocator.class);
    Mockito.when(nodeLocator.locate(Mockito.eq(descriptor))).thenReturn(
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
    final QueryRunner<?> queryRunner = passthroughRunner(descriptor.getRowSignature());
    return new TestClient(
        newClient(
            nodeLocator,
            directClientFactory,
            queryScheduler,
            Map.of(descriptor.getTableName(), descriptor),
            nonMatchingSelfNode()
        ),
        queryRunner
    );
  }

  private static TestClient makeClient(
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

  private static TestClient makeClient(
      final SystemTableDescriptor descriptor,
      final List<DiscoveryDruidNode> discoveryNodes,
      final Function<DruidServer, QueryRunner<ScanResultValue>> nodeRunnerFactory,
      final QueryRunner<?> queryRunner
  )
  {
    final SystemTableNodeLocator nodeLocator = Mockito.mock(SystemTableNodeLocator.class);
    Mockito.when(nodeLocator.locate(Mockito.eq(descriptor))).thenReturn(
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

    return new TestClient(
        newClient(
            nodeLocator,
            directClientFactory,
            Mockito.mock(QueryScheduler.class),
            Map.of(descriptor.getTableName(), descriptor),
            nonMatchingSelfNode()
        ),
        queryRunner
    );
  }

  /**
   * Creates a client that reads no system table locally, for tests whose nodes are all remote.
   */
  private static SystemTableQueryClient newClient(
      final SystemTableNodeLocator nodeLocator,
      final DirectDruidClientFactory directClientFactory,
      final QueryScheduler queryScheduler,
      final Map<String, SystemTableDescriptor> descriptors,
      final DruidNode selfNode
  )
  {
    return new SystemTableQueryClient(
        nodeLocator,
        directClientFactory,
        queryScheduler,
        Mockito.mock(SegmentWrangler.class),
        CONGLOMERATE.getConglomerate(),
        descriptors,
        new ServerConfig(),
        selfNode
    );
  }

  /**
   * A client paired with the runner that stands in for the Broker's walker. {@link #createRunner} resolves the query
   * with the client, as {@code NativeQueryMaker} does, and runs the resolved query with that runner.
   */
  private record TestClient(SystemTableQueryClient client, QueryRunner<?> queryRunner)
  {
    <T> QueryRunner<T> createRunner(final Query<T> query)
    {
      return createRunner(query, SystemTableAccess.ALL_ROWS);
    }

    /**
     * Gives each system table in the query the cluster scope, with the given access attached, as
     * {@code NativeQueryMaker} and {@code QueryLifecycle} do before the Broker walker resolves the query.
     */
    @SuppressWarnings("unchecked")
    <T> QueryRunner<T> createRunner(final Query<T> query, final SystemTableAccess access)
    {
      return (queryPlus, responseContext) -> {
        final SystemTableQueryClient.ResolvedQuery<T> resolvedQuery =
            client.resolve(query.withDataSource(authorizedAndGathered(query.getDataSource(), access)));
        return Sequences.withBaggage(
            ((QueryRunner<T>) queryRunner).run(queryPlus.withQuery(resolvedQuery.query()), responseContext),
            resolvedQuery.closer()
        );
      };
    }
  }

  private static <T> List<T> runToList(final TestClient client, final Query<T> query)
  {
    return client.createRunner(query)
                 .run(QueryPlus.wrap(query), ResponseContext.createEmpty())
                 .toList();
  }

  private static DataSource authorizedAndGathered(final DataSource dataSource, final SystemTableAccess access)
  {
    if (dataSource instanceof SystemTableDataSource systemTable) {
      return systemTable.withAccess(access).withScope(SystemTableDataSource.Scope.CLUSTER);
    }
    return dataSource.withChildren(
        dataSource.getChildren().stream().map(child -> authorizedAndGathered(child, access)).toList()
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

  /**
   * Resolves the query and returns the Scan sent to its only node.
   */
  private static ScanQuery captureNodeQuery(final SystemTableDescriptor descriptor, final Query<?> query)
  {
    final AtomicReference<ScanQuery> capturedNodeQuery = new AtomicReference<>();
    final TestClient client = makeClient(
        descriptor,
        List.of(testNode()),
        (queryPlus, responseContext) -> {
          capturedNodeQuery.set((ScanQuery) queryPlus.getQuery());
          return Sequences.empty();
        }
    );
    runToList(client, query);
    return capturedNodeQuery.get();
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
    Mockito.when(nodeLocator.locate(Mockito.eq(descriptor))).thenReturn(List.of(systemTableNode));
    final DirectDruidClientFactory directClientFactory = Mockito.mock(DirectDruidClientFactory.class);
    Mockito.when(directClientFactory.makeDirectClient(ArgumentMatchers.any())).thenAnswer(ignored -> {
      @SuppressWarnings("unchecked")
      final DirectDruidClient<ScanResultValue> directClient = Mockito.mock(DirectDruidClient.class);
      Mockito.when(directClient.run(ArgumentMatchers.any(), ArgumentMatchers.any())).thenAnswer(invocation -> {
        nodeQueries.add((ScanQuery) ((QueryPlus<?>) invocation.getArgument(0)).getQuery());
        return Sequences.empty();
      });
      return directClient;
    });
    final QueryRunner<?> queryRunner = (queryPlus, responseContext) -> Sequences.empty();
    final TestClient client = new TestClient(
        newClient(
            nodeLocator,
            directClientFactory,
            Mockito.mock(QueryScheduler.class),
            Map.of(descriptor.getTableName(), descriptor),
            nonMatchingSelfNode()
        ),
        queryRunner
    );
    final ScanQuery query = Druids.newScanQueryBuilder()
                                 .dataSource(dataSource)
                                 .eternityInterval()
                                 .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                 .filters(filter)
                                 .build();

    client.createRunner(query)
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
    final TestClient client = makeClient(
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

    client.createRunner(query)
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
      return (authenticationResult, authorizerMapper) -> SystemTableAccess.ALL_ROWS;
    }
  }
}
