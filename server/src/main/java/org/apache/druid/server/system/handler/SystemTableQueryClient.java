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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.client.DirectDruidClientFactory;
import org.apache.druid.client.DruidServer;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.LazySequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.operator.OperatorFactory;
import org.apache.druid.query.operator.ScanOperatorFactory;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.server.DataSourceQueryHandler;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.server.system.SystemTableNotLeaderException;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.apache.druid.server.system.table.SystemTableRoutingMode;
import org.apache.druid.utils.CloseableUtils;
import org.jboss.netty.channel.ChannelException;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Recursively replaces native system-table datasources with authorized inline rows fetched from nodes, then
 * executes the rebuilt query through the Broker's normal native-query walker.
 */
public class SystemTableQueryClient implements DataSourceQueryHandler
{
  private static final String SYSTEM_TABLE_TIER = "_system";

  private final SystemTableNodeLocator nodeLocator;
  private final DirectDruidClientFactory directDruidClientFactory;
  private final QueryScheduler queryScheduler;
  private final QuerySegmentWalker querySegmentWalker;
  private final Map<String, SystemTableDescriptor> tableDescriptors;
  private final AuthorizerMapper authorizerMapper;
  private final SystemTableQueryHandler localQueryHandler;
  private final AuthenticationResult escalatedAuthenticationResult;
  private final DruidNode selfNode;

  @Inject
  public SystemTableQueryClient(
      final SystemTableNodeLocator nodeLocator,
      final DirectDruidClientFactory directDruidClientFactory,
      final QueryScheduler queryScheduler,
      final QuerySegmentWalker querySegmentWalker,
      final Map<String, SystemTableDescriptor> tableDescriptors,
      final AuthorizerMapper authorizerMapper,
      final SystemTableQueryHandler localQueryHandler,
      final Escalator escalator,
      @Self final DruidNode selfNode
  )
  {
    this.nodeLocator = nodeLocator;
    this.directDruidClientFactory = directDruidClientFactory;
    this.queryScheduler = queryScheduler;
    this.querySegmentWalker = querySegmentWalker;
    this.tableDescriptors = tableDescriptors;
    this.authorizerMapper = authorizerMapper;
    this.localQueryHandler = localQueryHandler;
    this.escalatedAuthenticationResult = escalator.createEscalatedAuthenticationResult();
    this.selfNode = selfNode;
  }

  @Override
  public <T> QueryRunner<T> createRunner(
      final Query<T> query,
      final AuthenticationResult authenticationResult,
      final boolean executeLocally
  )
  {
    return (queryPlus, responseContext) -> {
      final NodeSequencesCloser nodeSequencesCloser = new NodeSequencesCloser(queryScheduler);
      try {
        final DataSource resolvedDataSource = resolveDataSource(
            query.getDataSource(),
            query,
            authenticationResult,
            responseContext,
            nodeSequencesCloser
        );
        final Query<T> resolvedQuery = query.withDataSource(resolvedDataSource).withOverriddenContext(
            Map.of(QueryContexts.QUERY_RESOURCE_ID, UUID.randomUUID().toString())
        );
        final QueryRunner<T> queryRunner = querySegmentWalker.getQueryRunnerForIntervals(
            resolvedQuery,
            resolvedQuery.getIntervals()
        );
        return Sequences.withBaggage(
            queryRunner.run(queryPlus.withQuery(resolvedQuery), responseContext),
            nodeSequencesCloser
        );
      }
      catch (Throwable t) {
        throw CloseableUtils.closeAndWrapInCatch(t, nodeSequencesCloser);
      }
    };
  }

  private DataSource resolveDataSource(
      final DataSource dataSource,
      final Query<?> owningQuery,
      final AuthenticationResult authenticationResult,
      final ResponseContext responseContext,
      final NodeSequencesCloser nodeSequencesCloser
  )
  {
    if (dataSource instanceof SystemTableDataSource systemTableDataSource) {
      return resolveSystemTableDataSource(
          systemTableDataSource,
          owningQuery,
          authenticationResult,
          responseContext,
          nodeSequencesCloser
      );
    }

    final Query<?> childOwningQuery = dataSource instanceof QueryDataSource queryDataSource
                                      ? queryDataSource.getQuery()
                                      : owningQuery;
    final List<DataSource> resolvedChildren = new ArrayList<>();
    for (final DataSource child : dataSource.getChildren()) {
      resolvedChildren.add(
          resolveDataSource(
              child,
              childOwningQuery,
              authenticationResult,
              responseContext,
              nodeSequencesCloser
          )
      );
    }
    return dataSource.withChildren(resolvedChildren);
  }

  private InlineDataSource resolveSystemTableDataSource(
      final SystemTableDataSource dataSource,
      final Query<?> owningQuery,
      final AuthenticationResult authenticationResult,
      final ResponseContext responseContext,
      final NodeSequencesCloser nodeSequencesCloser
  )
  {
    final SystemTableDescriptor descriptor = tableDescriptors.get(dataSource.getTable());
    if (descriptor == null) {
      throw new ISE("No routing descriptor is registered for system table[%s]", dataSource.getTable());
    }

    final ScanQuery nodeQuery = makeNodeQuery(dataSource, descriptor, owningQuery);
    final List<QueryRunner<ScanResultValue>> nodeRunners = makeNodeRunners(
        nodeQuery,
        descriptor,
        nodeSequencesCloser
    );
    if (nodeRunners.isEmpty() && !descriptor.isEmptyDiscoveryAllowed()) {
      throw new ISE("No node is available to serve system table[%s]", dataSource.getTable());
    }

    /*
     * Keep the scan result transport lazy. The node response is a Sequence, so this lets the local query
     * runner consume and process rows while the HTTP response is still arriving instead of materializing every
     * node row in two Broker-side lists. The scan transport is intentionally kept behind this adapter so a
     * future node-side aggregation mode can use a different result adapter without changing routing or
     * authorization orchestration here.
     */
    final NodeSequenceCloser nodeSequenceCloser = new NodeSequenceCloser();
    nodeSequencesCloser.add(nodeSequenceCloser);
    final Iterable<Object[]> nodeRows = scanNodeRows(
        nodeRunners,
        nodeQuery,
        responseContext,
        nodeSequenceCloser
    );
    final Iterable<Object[]> authorizedRows = descriptor.getRowAuthorizer().filterAuthorizedRows(
        nodeRows,
        authenticationResult,
        authorizerMapper
    );
    final Iterable<Object[]> queryRows = requiresMaterializedRows(owningQuery)
                                         ? materializeRows(authorizedRows)
                                         : authorizedRows;
    return InlineDataSource.fromIterable(queryRows, descriptor.getRowSignature());
  }

  private ScanQuery makeNodeQuery(
      final SystemTableDataSource dataSource,
      final SystemTableDescriptor descriptor,
      final Query<?> owningQuery
  )
  {
    final Map<String, Object> nodeContext = new LinkedHashMap<>(owningQuery.getContext());
    final DimFilter nodeFilter = nodeFilter(dataSource, owningQuery);
    nodeContext.put(DirectDruidClient.QUERY_FAIL_TIME, getNodeFailTime(owningQuery));
    // The node endpoint always returns plain ScanResultValue objects. bySegment belongs to the user-facing query and
    // would make DirectDruidClient deserialize this private transport using an incompatible result type.
    nodeContext.put(QueryContexts.BY_SEGMENT_KEY, false);
    return Druids.newScanQueryBuilder()
                 .dataSource(dataSource)
                 .eternityInterval()
                 .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                 .limit(Long.MAX_VALUE)
                 .filters(nodeFilter)
                 .virtualColumns(nodeVirtualColumns(dataSource, owningQuery, nodeFilter))
                 .columns(descriptor.getRowSignature())
                 .context(nodeContext)
                 .build();
  }

  private List<QueryRunner<ScanResultValue>> makeNodeRunners(
      final ScanQuery nodeQuery,
      final SystemTableDescriptor descriptor,
      final NodeSequencesCloser nodeSequencesCloser
  )
  {
    final List<QueryRunner<ScanResultValue>> nodeRunners = new ArrayList<>();
    for (final SystemTableNode node : nodeLocator.locate(descriptor, nodeQuery)) {
      nodeRunners.add(
          (queryPlus, responseContext) -> recoverNodeFailure(
              () -> runNodeQuery(nodeQuery, node, nodeSequencesCloser, queryPlus, responseContext),
              node,
              descriptor,
              descriptor.getRoutingMode() == SystemTableRoutingMode.LEADER_ONLY
              ? () -> {
                final SystemTableNode currentLeader = Iterables.getOnlyElement(
                    nodeLocator.locate(descriptor, nodeQuery)
                );
                return new NodeQuerySequence(
                    currentLeader,
                    runNodeQuery(nodeQuery, currentLeader, nodeSequencesCloser, queryPlus, responseContext)
                );
              }
              : null
          )
      );
    }
    return nodeRunners;
  }

  private Sequence<ScanResultValue> runNodeQuery(
      final ScanQuery nodeQuery,
      final SystemTableNode node,
      final NodeSequencesCloser nodeSequencesCloser,
      final QueryPlus<ScanResultValue> queryPlus,
      final ResponseContext responseContext
  )
  {
    final String nodeResourceId = UUID.randomUUID().toString();
    final String nodeQueryId = SystemTableDataSource.NODE_QUERY_ID_PREFIX + UUID.randomUUID();
    final ScanQuery subNativeQuery = nodeQuery.withOverriddenContext(
        Map.of(
            BaseQuery.QUERY_ID,
            nodeQueryId,
            QueryContexts.QUERY_RESOURCE_ID,
            nodeResourceId
        )
    );
    final QueryRunner<ScanResultValue> nodeRunner;
    if (SystemTableNodeLocator.sameServer(
        selfNode.getUriToUse(),
        node.getDiscoveryNode().getDruidNode().getUriToUse()
    )) {
      // Invoke the raw local handler rather than the Broker handler. This avoids an HTTP request back to this
      // Broker and, more importantly, prevents the local request from entering node fanout recursively.
      nodeRunner = localQueryHandler.createRunner(subNativeQuery, escalatedAuthenticationResult, true);
    } else {
      nodeRunner = directDruidClientFactory.makeDirectClient(toDruidServer(node.getDiscoveryNode()));
      nodeSequencesCloser.addQueryId(nodeQueryId);
    }
    return nodeRunner.run(queryPlus.withQuery(subNativeQuery), responseContext);
  }

  /**
   * Window operators consume a {@code RowsAndColumns} view of the segment. The current row-based segment can only
   * provide that view when its rows are list-backed, so preserve the existing materialized path for those queries.
   */
  private static boolean requiresMaterializedRows(final Query<?> query)
  {
    return query instanceof WindowOperatorQuery;
  }

  /**
   * Eagerly consumes the lazy, single-pass node response into an {@link ArrayList} without changing its rows.
   *
   * <p>This representation change is required by {@link org.apache.druid.segment.InlineSegmentWrangler}: an inline
   * datasource backed specifically by an {@code ArrayList} becomes an {@link org.apache.druid.segment.ArrayListSegment},
   * which can provide the {@code RowsAndColumns} view required by window operators. Other query types retain the lazy
   * iterable so the Broker can process rows while node responses are still arriving.</p>
   */
  private static Iterable<Object[]> materializeRows(final Iterable<Object[]> rows)
  {
    final List<Object[]> materializedRows = new ArrayList<>();
    rows.forEach(materializedRows::add);
    return materializedRows;
  }

  /**
   * Creates a lazy, single-pass view of the rows returned by the node scan runners.
   *
   * <p>The view is single-pass because a node response is backed by a live HTTP stream. The normal native query
   * path consumes an {@link InlineDataSource} once per query. Re-iterating would otherwise issue duplicate node
   * requests or retain all rows to support replay.</p>
   */
  static Iterable<Object[]> scanNodeRows(
      final List<QueryRunner<ScanResultValue>> nodeRunners,
      final Query<ScanResultValue> nodeQuery,
      final ResponseContext responseContext
  )
  {
    return scanNodeRows(
        nodeRunners,
        nodeQuery,
        responseContext,
        new NodeSequenceCloser()
    );
  }

  private static Iterable<Object[]> scanNodeRows(
      final List<QueryRunner<ScanResultValue>> nodeRunners,
      final Query<ScanResultValue> nodeQuery,
      final ResponseContext responseContext,
      final NodeSequenceCloser nodeSequenceCloser
  )
  {
    final Sequence<Object[]> rows = new LazySequence<>(() -> {
      final List<Sequence<ScanResultValue>> nodeSequences = new ArrayList<>();
      for (final QueryRunner<ScanResultValue> nodeRunner : nodeRunners) {
        // DirectDruidClient.run starts its asynchronous HTTP request. Start every request before waiting for any
        // response so slow nodes do not serialize the entire system-table query.
        nodeSequences.add(nodeRunner.run(QueryPlus.wrap(nodeQuery), responseContext));
      }
      // Node scans have no cross-node ordering. Consume them in discovery order so a ready node can yield rows without
      // MergeSequence first initializing every other node sequence. The HTTP requests were all started above.
      return Sequences.concat(nodeSequences).flatMap(SystemTableQueryClient::rowsFromScanResult);
    });
    return sequenceAsIterable(rows, nodeSequenceCloser);
  }

  private static Sequence<ScanResultValue> recoverNodeFailure(
      final Supplier<Sequence<ScanResultValue>> sequenceSupplier,
      final SystemTableNode node,
      final SystemTableDescriptor descriptor,
      @Nullable final Supplier<NodeQuerySequence> leaderRetrySupplier
  )
  {
    final Sequence<ScanResultValue> sequence;
    try {
      sequence = sequenceSupplier.get();
    }
    catch (Exception failure) {
      if (leaderRetrySupplier != null && isLeaderChangeFailure(failure)) {
        try {
          final NodeQuerySequence retry = leaderRetrySupplier.get();
          return recoveringSequence(retry, descriptor, null);
        }
        catch (Exception retryFailure) {
          return failureSequence(retryFailure, node, descriptor);
        }
      }
      return failureSequence(failure, node, descriptor);
    }
    return recoveringSequence(new NodeQuerySequence(node, sequence), descriptor, leaderRetrySupplier);
  }

  private static Sequence<ScanResultValue> recoveringSequence(
      final NodeQuerySequence nodeQuerySequence,
      final SystemTableDescriptor descriptor,
      @Nullable final Supplier<NodeQuerySequence> leaderRetrySupplier
  )
  {
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<ScanResultValue, RecoveringNodeIterator>()
        {
          @Override
          public RecoveringNodeIterator make()
          {
            return new RecoveringNodeIterator(nodeQuerySequence, descriptor, leaderRetrySupplier);
          }

          @Override
          public void cleanup(final RecoveringNodeIterator iterator)
          {
            CloseableUtils.closeAndWrapExceptions(iterator);
          }
        }
    );
  }

  private static Sequence<ScanResultValue> failureSequence(
      final Exception failure,
      final SystemTableNode node,
      final SystemTableDescriptor descriptor
  )
  {
    if (!isNodeAvailabilityFailure(failure)) {
      throw propagate(failure);
    }
    return descriptor.getNodeFailureRow(
        node.getDiscoveryNode().getDruidNode(),
        node.getNodeRoles(),
        failure
    ).map(
        row -> Sequences.simple(
            List.of(
                new ScanResultValue(
                    null,
                    descriptor.getRowSignature().getColumnNames(),
                    List.of((Object) row)
                )
            )
        )
    ).orElseThrow(() -> QueryInterruptedException.wrapIfNeeded(failure));
  }

  private static boolean isNodeAvailabilityFailure(final Throwable failure)
  {
    Throwable cause = failure;
    while (cause != null) {
      if (cause instanceof SocketException
          || cause instanceof UnknownHostException
          || cause instanceof EOFException
          || cause instanceof ClosedChannelException
          || cause instanceof ChannelException) {
        return true;
      }
      cause = cause.getCause();
    }
    return false;
  }

  private static boolean isLeaderChangeFailure(final Throwable failure)
  {
    Throwable cause = failure;
    while (cause != null) {
      if (cause instanceof SystemTableNotLeaderException
          || cause instanceof QueryException
             && SystemTableNotLeaderException.class.getName().equals(((QueryException) cause).getErrorClass())) {
        return true;
      }
      cause = cause.getCause();
    }
    return false;
  }

  private static RuntimeException propagate(final Exception failure)
  {
    if (failure instanceof RuntimeException runtimeException) {
      return runtimeException;
    } else if (failure instanceof java.util.concurrent.TimeoutException) {
      return new QueryTimeoutException(failure.getMessage());
    } else {
      if (failure instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return QueryInterruptedException.wrapIfNeeded(failure);
    }
  }

  private static Sequence<Object[]> rowsFromScanResult(final ScanResultValue scanResult)
  {
    return Sequences.simple((List<?>) scanResult.getEvents())
                    .map(event -> event instanceof Object[] ? (Object[]) event : ((List<?>) event).toArray());
  }

  private static long getNodeFailTime(final Query<?> query)
  {
    final long existingFailTime = query.context().getLong(DirectDruidClient.QUERY_FAIL_TIME, -1L);
    if (existingFailTime > 0) {
      return existingFailTime;
    } else if (query.context().hasTimeout()) {
      return System.currentTimeMillis() + query.context().getTimeout();
    } else {
      return JodaUtils.MAX_INSTANT;
    }
  }

  /**
   * Adapts a Druid {@link Sequence} to the lazy {@link Iterable} accepted by {@link InlineDataSource}.
   *
   * <p>The iterator owns the sequence yielder and closes it when the sequence is exhausted. This is important for
   * direct node clients because closing the yielder closes the underlying response stream and allows an early
   * terminating Broker query to cancel the node request.</p>
   */
  private static <T> Iterable<T> sequenceAsIterable(
      final Sequence<T> sequence,
      final NodeSequenceCloser nodeSequenceCloser
  )
  {
    return new Iterable<>()
    {
      private boolean iterated;

      @Override
      public Iterator<T> iterator()
      {
        if (iterated) {
          throw new ISE("A native system table result sequence can only be consumed once");
        }
        iterated = true;

        return new Iterator<>()
        {
          @Nullable
          private Yielder<T> yielder = Yielders.each(sequence);
          private boolean closed;

          {
            nodeSequenceCloser.setYielder(yielder);
          }

          @Override
          public boolean hasNext()
          {
            if (yielder == null) {
              return false;
            }
            if (yielder.isDone()) {
              close();
              return false;
            }
            return true;
          }

          @Override
          public T next()
          {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            final Yielder<T> currentYielder = Preconditions.checkNotNull(yielder);
            final T value = currentYielder.get();
            yielder = currentYielder.next(null);
            nodeSequenceCloser.setYielder(yielder);
            return value;
          }

          private void close()
          {
            if (!closed) {
              closed = true;
              yielder = null;
              nodeSequenceCloser.close();
            }
          }
        };
      }
    };
  }

  private static class NodeSequenceCloser implements Closeable
  {
    @Nullable
    private Yielder<?> yielder;
    private boolean closed;

    synchronized void setYielder(final Yielder<?> newYielder)
    {
      if (closed) {
        CloseableUtils.closeAndWrapExceptions(newYielder);
      } else {
        yielder = newYielder;
      }
    }

    @Override
    public synchronized void close()
    {
      if (!closed) {
        closed = true;
        final Yielder<?> currentYielder = yielder;
        yielder = null;
        CloseableUtils.closeAndWrapExceptions(currentYielder);
      }
    }
  }

  private static class NodeSequencesCloser implements Closeable
  {
    private final List<NodeSequenceCloser> nodeSequenceClosers = new ArrayList<>();
    private final List<String> nodeQueryIds = new ArrayList<>();
    private final QueryScheduler queryScheduler;
    private boolean closed;

    private NodeSequencesCloser(final QueryScheduler queryScheduler)
    {
      this.queryScheduler = queryScheduler;
    }

    synchronized void addQueryId(final String nodeQueryId)
    {
      if (closed) {
        queryScheduler.cancelQuery(nodeQueryId);
      } else {
        nodeQueryIds.add(nodeQueryId);
      }
    }

    synchronized void add(final NodeSequenceCloser nodeSequenceCloser)
    {
      if (closed) {
        CloseableUtils.closeAndWrapExceptions(nodeSequenceCloser);
      } else {
        nodeSequenceClosers.add(nodeSequenceCloser);
      }
    }

    @Override
    public synchronized void close() throws IOException
    {
      if (!closed) {
        closed = true;
        try {
          CloseableUtils.closeAll(nodeSequenceClosers);
        }
        finally {
          nodeSequenceClosers.clear();
          nodeQueryIds.forEach(queryScheduler::cancelQuery);
          nodeQueryIds.clear();
        }
      }
    }
  }

  private static class RecoveringNodeIterator implements Iterator<ScanResultValue>, Closeable
  {
    private final SystemTableDescriptor descriptor;
    @Nullable
    private final Supplier<NodeQuerySequence> leaderRetrySupplier;

    private NodeQuerySequence nodeQuerySequence;

    @Nullable
    private Yielder<ScanResultValue> yielder;
    @Nullable
    private ScanResultValue failureResult;
    private boolean started;
    private boolean finished;
    private boolean retriedLeader;
    private boolean emittedResult;

    private RecoveringNodeIterator(
        final NodeQuerySequence nodeQuerySequence,
        final SystemTableDescriptor descriptor,
        @Nullable final Supplier<NodeQuerySequence> leaderRetrySupplier
    )
    {
      this.nodeQuerySequence = nodeQuerySequence;
      this.descriptor = descriptor;
      this.leaderRetrySupplier = leaderRetrySupplier;
    }

    @Override
    public boolean hasNext()
    {
      if (failureResult != null) {
        return true;
      }
      if (finished) {
        return false;
      }

      try {
        if (!started) {
          started = true;
          yielder = Yielders.each(nodeQuerySequence.sequence());
        }
        if (Preconditions.checkNotNull(yielder).isDone()) {
          finished = true;
          close();
          return false;
        }
        return true;
      }
      catch (Exception e) {
        return recoverOrThrow(e) ? hasNext() : true;
      }
    }

    @Override
    public ScanResultValue next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      if (failureResult != null) {
        final ScanResultValue result = failureResult;
        failureResult = null;
        finished = true;
        return result;
      }

      final Yielder<ScanResultValue> currentYielder = Preconditions.checkNotNull(yielder);
      final ScanResultValue result;
      try {
        result = currentYielder.get();
        emittedResult = true;
      }
      catch (Exception e) {
        if (recoverOrThrow(e)) {
          return next();
        }
        return next();
      }
      try {
        yielder = currentYielder.next(null);
      }
      catch (Exception e) {
        recoverOrThrow(e);
      }
      return result;
    }

    private boolean recoverOrThrow(final Exception originalFailure)
    {
      Exception failure = originalFailure;
      if (!emittedResult && !retriedLeader && leaderRetrySupplier != null && isLeaderChangeFailure(failure)) {
        retriedLeader = true;
        try {
          close();
          nodeQuerySequence = leaderRetrySupplier.get();
          started = false;
          finished = false;
          return true;
        }
        catch (Exception retryFailure) {
          failure = retryFailure;
        }
      }
      if (!isNodeAvailabilityFailure(failure)) {
        throw CloseableUtils.closeAndWrapInCatch(failure, this);
      }
      final Optional<Object[]> failureRow = descriptor.getNodeFailureRow(
          nodeQuerySequence.node().getDiscoveryNode().getDruidNode(),
          nodeQuerySequence.node().getNodeRoles(),
          failure
      );
      if (failureRow.isEmpty()) {
        throw CloseableUtils.closeAndWrapInCatch(failure, this);
      }
      try {
        close();
      }
      catch (RuntimeException closeException) {
        failure.addSuppressed(closeException);
      }
      failureResult = new ScanResultValue(
          null,
          descriptor.getRowSignature().getColumnNames(),
          List.of((Object) failureRow.get())
      );
      return false;
    }

    @Override
    public void close()
    {
      final Yielder<ScanResultValue> currentYielder = yielder;
      yielder = null;
      CloseableUtils.closeAndWrapExceptions(currentYielder);
    }
  }

  private record NodeQuerySequence(SystemTableNode node, Sequence<ScanResultValue> sequence)
  {
  }

  @Nullable
  private static DimFilter nodeFilter(final SystemTableDataSource dataSource, final Query<?> query)
  {
    // Push into the remote scan only when this query directly owns the system table. A join or other composite filter
    // remains on the resolved Broker query, where the normal native query machinery can apply it with full datasource
    // semantics; it cannot safely be attributed to an individual remote leaf here.
    if (!dataSource.equals(query.getDataSource())) {
      return null;
    }

    final List<DimFilter> filters = new ArrayList<>();
    if (query.getFilter() != null) {
      filters.add(query.getFilter());
    }
    if (query instanceof WindowOperatorQuery) {
      for (final OperatorFactory operator : ((WindowOperatorQuery) query).getLeafOperators()) {
        if (operator instanceof ScanOperatorFactory && ((ScanOperatorFactory) operator).getFilter() != null) {
          filters.add(((ScanOperatorFactory) operator).getFilter());
        }
      }
    }
    if (filters.isEmpty()) {
      return null;
    } else if (filters.size() == 1) {
      return filters.get(0);
    } else {
      return new AndDimFilter(filters);
    }
  }

  private static VirtualColumns nodeVirtualColumns(
      final SystemTableDataSource dataSource,
      final Query<?> query,
      @Nullable final DimFilter nodeFilter
  )
  {
    if (!dataSource.equals(query.getDataSource()) || nodeFilter == null) {
      return VirtualColumns.EMPTY;
    }

    final List<VirtualColumn> virtualColumns = new ArrayList<>(List.of(query.getVirtualColumns().getVirtualColumns()));
    if (query instanceof WindowOperatorQuery) {
      for (final OperatorFactory operator : ((WindowOperatorQuery) query).getLeafOperators()) {
        if (operator instanceof ScanOperatorFactory) {
          virtualColumns.addAll(List.of(((ScanOperatorFactory) operator).getVirtualColumns().getVirtualColumns()));
        }
      }
    }
    return VirtualColumns.create(virtualColumns);
  }

  private static DruidServer toDruidServer(final DiscoveryDruidNode discoveryNode)
  {
    final DruidNode node = discoveryNode.getDruidNode();
    return new DruidServer(
        node.getHostAndPortToUse(),
        node.getHostAndPort(),
        node.getHostAndTlsPort(),
        0,
        null,
        ServerType.HISTORICAL,
        SYSTEM_TABLE_TIER,
        0
    );
  }
}
