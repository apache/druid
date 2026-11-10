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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import io.netty.channel.ChannelException;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.client.DirectDruidClientFactory;
import org.apache.druid.client.DruidServer;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.LazySequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.SystemTableAccess;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.union.UnionQuery;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentWrangler;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.SetAndVerifyContextQueryRunner;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Gathers the rows of system tables from the services that serve them, for the Broker.
 *
 * <p>{@link #resolve} replaces each system table with the {@link SystemTableDataSource.Scope#CLUSTER} scope with an
 * {@link InlineDataSource} holding the rows that the caller may read, gathered with a Scan of the table on each
 * service. The caller then runs the resolved query as usual. Services read the table with an escalated identity, so
 * the rows are filtered here with the caller's {@link SystemTableAccess}, which the Broker attached when it authorized
 * the query.</p>
 */
public class SystemTableQueryClient
{
  private static final String SYSTEM_TABLE_TIER = "_system";

  private final SystemTableNodeLocator nodeLocator;
  private final DirectDruidClientFactory directDruidClientFactory;
  private final QueryScheduler queryScheduler;
  private final SegmentWrangler segmentWrangler;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final Map<String, SystemTableDescriptor> tableDescriptors;
  private final ServerConfig serverConfig;
  private final DruidNode selfNode;

  @Inject
  public SystemTableQueryClient(
      final SystemTableNodeLocator nodeLocator,
      final DirectDruidClientFactory directDruidClientFactory,
      final QueryScheduler queryScheduler,
      final SegmentWrangler segmentWrangler,
      final QueryRunnerFactoryConglomerate conglomerate,
      final Map<String, SystemTableDescriptor> tableDescriptors,
      final ServerConfig serverConfig,
      @Self final DruidNode selfNode
  )
  {
    this.nodeLocator = nodeLocator;
    this.directDruidClientFactory = directDruidClientFactory;
    this.queryScheduler = queryScheduler;
    this.segmentWrangler = segmentWrangler;
    this.conglomerate = conglomerate;
    this.tableDescriptors = tableDescriptors;
    this.serverConfig = serverConfig;
    this.selfNode = selfNode;
  }

  /**
   * Replaces each system table in the query that has the cluster scope with the rows of the table that the caller may
   * read. The rows are gathered from every service before this method returns. The returned closer cancels any node
   * queries that are still running; the caller must close it once the resolved query is done.
   */
  public <T> ResolvedQuery<T> resolve(final Query<T> query)
  {
    // Node queries derive their timeout and scatter-gather limit from the query, so apply the server's limits first.
    final Query<T> limitedQuery = new SetAndVerifyContextQueryRunner<T>(serverConfig, null)
        .withTimeoutAndMaxScatterGatherBytes(query, serverConfig);
    final NodeSequencesCloser nodeSequencesCloser = new NodeSequencesCloser(queryScheduler);
    try {
      final DataSource resolvedDataSource = resolveDataSource(
          limitedQuery.getDataSource(),
          limitedQuery,
          nodeSequencesCloser
      );
      return new ResolvedQuery<>(limitedQuery.withDataSource(resolvedDataSource), nodeSequencesCloser);
    }
    catch (Throwable t) {
      throw CloseableUtils.closeAndWrapInCatch(t, nodeSequencesCloser);
    }
  }

  /**
   * A query whose system tables were replaced with inline rows.
   *
   * @param query  the resolved query
   * @param closer cancels the node queries that are still running; close it once the query is done
   */
  public record ResolvedQuery<T>(Query<T> query, Closeable closer)
  {
  }

  private DataSource resolveDataSource(
      final DataSource dataSource,
      final Query<?> owningQuery,
      final NodeSequencesCloser nodeSequencesCloser
  )
  {
    if (dataSource instanceof SystemTableDataSource systemTableDataSource) {
      if (systemTableDataSource.getScope() != SystemTableDataSource.Scope.CLUSTER) {
        return systemTableDataSource;
      }
      return resolveSystemTableDataSource(
          systemTableDataSource,
          owningQuery,
          nodeSequencesCloser
      );
    }

    final Query<?> childOwningQuery;
    final DataSource resolvedParent;
    if (dataSource instanceof QueryDataSource queryDataSource) {
      final Query<?> childQuery = queryDataSource.getQuery();
      // An inner query may tighten the outer guardrails, but must not discard them before node fanout.
      childOwningQuery = withGuardrails(childQuery, owningQuery);
      if (childQuery instanceof UnionQuery unionQuery) {
        // Each child of a union is the datasource of one of its queries, which owns that child. Guard the original
        // queries: the guarded union has given every query the union's limits, losing any tighter limits of its own.
        final List<Query<?>> resolvedArms = new ArrayList<>();
        for (final Query<?> arm : unionQuery.getQueries()) {
          final Query<?> guardedArm = withGuardrails(arm, childOwningQuery);
          resolvedArms.add(
              guardedArm.withDataSource(
                  resolveDataSource(guardedArm.getDataSource(), guardedArm, nodeSequencesCloser)
              )
          );
        }
        return new QueryDataSource(new UnionQuery(resolvedArms, childOwningQuery.getContext()));
      }
      resolvedParent = new QueryDataSource(childOwningQuery);
    } else {
      childOwningQuery = owningQuery;
      resolvedParent = dataSource;
    }
    final List<DataSource> resolvedChildren = new ArrayList<>();
    for (final DataSource child : dataSource.getChildren()) {
      resolvedChildren.add(
          resolveDataSource(
              child,
              childOwningQuery,
              nodeSequencesCloser
          )
      );
    }
    return resolvedParent.withChildren(resolvedChildren);
  }

  /**
   * Returns the inner query with the outer query's deadline and scatter-gather limit, where they are tighter.
   */
  private static Query<?> withGuardrails(final Query<?> innerQuery, final Query<?> outerQuery)
  {
    return innerQuery.withOverriddenContext(
        QueryContext.ofMap(
            QueryContextParameters.QUERY_FAIL_TIME,
            Math.min(getNodeFailTime(outerQuery), getNodeFailTime(innerQuery)),
            QueryContextParameters.MAX_SCATTER_GATHER_BYTES,
            Math.min(outerQuery.context().getMaxScatterGatherBytes(), innerQuery.context().getMaxScatterGatherBytes())
        )
    );
  }

  private InlineDataSource resolveSystemTableDataSource(
      final SystemTableDataSource dataSource,
      final Query<?> owningQuery,
      final NodeSequencesCloser nodeSequencesCloser
  )
  {
    final SystemTableDescriptor descriptor = tableDescriptors.get(dataSource.getTable());
    if (descriptor == null) {
      throw DruidException.defensive("No routing descriptor is registered for system table[%s]", dataSource.getTable());
    }

    final SystemTableAccess access = dataSource.getAccess();
    if (access == null) {
      throw DruidException.defensive("System table[%s] was not authorized before execution", dataSource.getTable());
    }
    final ScanQuery nodeQuery = makeNodeQuery(dataSource, descriptor, owningQuery);
    final List<QueryRunner<ScanResultValue>> nodeRunners = makeNodeRunners(
        nodeQuery,
        descriptor,
        nodeSequencesCloser
    );
    if (nodeRunners.isEmpty() && !descriptor.isEmptyDiscoveryAllowed()) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.SERVICE_UNAVAILABLE)
                          .build("No node is available to serve system table[%s]", dataSource.getTable());
    }

    final NodeSequenceCloser nodeSequenceCloser = new NodeSequenceCloser();
    nodeSequencesCloser.add(nodeSequenceCloser);
    // Node scans get their own response context. Sharing the caller's would add the node scans' row counts to the
    // Broker query's, and a Scan with a limit returns no rows once that count reaches the limit. For example, the
    // node scans of a join's right side would empty a limited Scan over the join before it read its left side.
    final Iterable<Object[]> nodeRows = scanNodeRows(
        nodeRunners,
        nodeQuery,
        DirectDruidClient.makeResponseContextForQuery(),
        nodeSequenceCloser
    );
    // Collect the rows into a list: an inline datasource backed by a list can be read more than once, and window
    // queries can read it as RowsAndColumns. The node scans' scatter-gather limit bounds the rows held here, per
    // table, and the Broker's own rows aren't counted against it.
    final ArrayList<Object[]> rows = new ArrayList<>();
    access.filter(nodeRows).forEach(rows::add);
    return InlineDataSource.fromIterable(rows, descriptor.getRowSignature());
  }

  private ScanQuery makeNodeQuery(
      final SystemTableDataSource dataSource,
      final SystemTableDescriptor descriptor,
      final Query<?> owningQuery
  )
  {
    // The node endpoint always returns plain ScanResultValue objects. bySegment belongs to the user-facing query and
    // would make DirectDruidClient deserialize this private transport using an incompatible result type.
    return Druids.newScanQueryBuilder()
                 .dataSource(dataSource)
                 .eternityInterval()
                 .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                 .limit(Long.MAX_VALUE)
                 .filters(getNodeScanFilter(dataSource, descriptor, owningQuery))
                 .columns(descriptor.getRowSignature())
                 .context(owningQuery.withOverriddenContext(
                     QueryContext.ofMap(
                         QueryContextParameters.QUERY_FAIL_TIME,
                         getNodeFailTime(owningQuery),
                         QueryContextParameters.BY_SEGMENT,
                         false
                     )
                 ).getContext())
                 .build();
  }

  private List<QueryRunner<ScanResultValue>> makeNodeRunners(
      final ScanQuery nodeQuery,
      final SystemTableDescriptor descriptor,
      final NodeSequencesCloser nodeSequencesCloser
  )
  {
    final List<QueryRunner<ScanResultValue>> nodeRunners = new ArrayList<>();
    for (final SystemTableNode node : nodeLocator.locate(descriptor)) {
      nodeRunners.add(
          (queryPlus, responseContext) -> recoverNodeFailure(
              () -> runNodeQuery(nodeQuery, node, nodeSequencesCloser, queryPlus, responseContext),
              node,
              descriptor
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
        QueryContext.ofMap(
            QueryContextParameters.QUERY_ID,
            nodeQueryId,
            QueryContextParameters.QUERY_RESOURCE_ID,
            nodeResourceId
        )
    );
    if (SystemTableNodeLocator.sameServer(
        selfNode.getUriToUse(),
        node.getDiscoveryNode().getDruidNode().getUriToUse()
    )) {
      // Read this Broker's rows directly rather than over HTTP. Like the other services, which read the table with an
      // escalated identity, return every row: the rows are authorized once they are gathered. This doesn't go through
      // a walker, so the node scan doesn't take a second scheduler lane besides the query's own.
      final SystemTableDataSource localDataSource =
          ((SystemTableDataSource) subNativeQuery.getDataSource()).withAccess(SystemTableAccess.ALL_ROWS);
      final ScanQuery localQuery = subNativeQuery.withDataSource(localDataSource);
      final Segment segment = Iterables.getOnlyElement(
          segmentWrangler.getSegmentsForIntervals(localDataSource, localQuery.getIntervals())
      );
      final QueryRunnerFactory<ScanResultValue, ScanQuery> factory = conglomerate.findFactory(localQuery);
      // The Scan runner reads the deadline from the response context, and has none without it.
      if (responseContext.getTimeoutTime() == null) {
        responseContext.putTimeoutTime(getNodeFailTime(localQuery));
      }
      return factory.createRunner(segment).run(queryPlus.withQuery(localQuery), responseContext);
    }

    final QueryRunner<ScanResultValue> nodeRunner =
        directDruidClientFactory.makeDirectClient(toDruidServer(node.getDiscoveryNode()));
    nodeSequencesCloser.addQueryId(nodeQueryId);
    return nodeRunner.run(queryPlus.withQuery(subNativeQuery), responseContext);
  }

  /**
   * Returns the rows returned by the node scan runners, which can be read once. Reading them starts every node scan
   * before waiting for any response, and the rows are read while the responses arrive.
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
      final SystemTableDescriptor descriptor
  )
  {
    final Sequence<ScanResultValue> sequence;
    try {
      sequence = sequenceSupplier.get();
    }
    catch (Exception failure) {
      return failureSequence(failure, node, descriptor);
    }
    return recoveringSequence(new NodeQuerySequence(node, sequence), descriptor);
  }

  private static Sequence<ScanResultValue> recoveringSequence(
      final NodeQuerySequence nodeQuerySequence,
      final SystemTableDescriptor descriptor
  )
  {
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<ScanResultValue, RecoveringNodeIterator>()
        {
          @Override
          public RecoveringNodeIterator make()
          {
            return new RecoveringNodeIterator(nodeQuerySequence, descriptor);
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

  private static RuntimeException propagate(final Exception failure)
  {
    if (failure instanceof RuntimeException runtimeException) {
      return runtimeException;
    } else if (failure instanceof TimeoutException) {
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
    final long existingFailTime = query.context().getOrDefault(QueryContextParameters.QUERY_FAIL_TIME, -1L);
    if (existingFailTime > 0) {
      return existingFailTime;
    } else if (query.context().hasTimeout()) {
      final long failTime = System.currentTimeMillis() + query.context().getTimeout();
      return failTime <= 0 ? Long.MAX_VALUE : failTime;
    } else {
      return JodaUtils.MAX_INSTANT;
    }
  }

  /**
   * Adapts a Druid {@link Sequence} to an {@link Iterable} that can be read once.
   *
   * <p>The iterator owns the sequence yielder and closes it when the sequence is exhausted, or registers it with the
   * closer if reading stops early, so the node response streams are always closed.</p>
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
          throw DruidException.defensive("A native system table result sequence can only be consumed once");
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
    private final NodeQuerySequence nodeQuerySequence;

    @Nullable
    private Yielder<ScanResultValue> yielder;
    @Nullable
    private ScanResultValue failureResult;
    private boolean started;
    private boolean finished;

    private RecoveringNodeIterator(
        final NodeQuerySequence nodeQuerySequence,
        final SystemTableDescriptor descriptor
    )
    {
      this.nodeQuerySequence = nodeQuerySequence;
      this.descriptor = descriptor;
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
        // A failure row is now pending.
        recoverOrThrow(e);
        return true;
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
      }
      catch (Exception e) {
        // A failure row is now pending, and next() serves it.
        recoverOrThrow(e);
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

    /**
     * Turns a failure to reach the node into the table's failure row, which {@link #next()} returns. Other failures,
     * and failures of tables without failure rows, fail the query.
     */
    private void recoverOrThrow(final Exception failure)
    {
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

  /**
   * Returns the filter to put on the Scan sent to each node, or null: the part of the query filter that the nodes can
   * apply themselves. It doesn't choose which nodes are queried; {@link SystemTableNodeLocator} does. Filtering on the
   * nodes only reduces the rows they return: the resolved query applies its whole filter to the gathered rows.
   *
   * <p>Only conjuncts that read columns of the table are pushed down, and only for a query that reads the table
   * directly. Virtual columns are never pushed down: a virtual column can replace a column of the table, and the
   * gathered rows must hold the table's own values, which are authorized after they are gathered.</p>
   *
   * <p>For example, take a Scan on {@code sys.server_properties} with the virtual column
   * {@code v0 = upper("property")} and this filter:</p>
   *
   * <pre>
   * AND(
   *   service_name == 'druid/broker',   // reads service_name, a column of the table: pushed down
   *   v0 == 'DRUID.HOST'                // reads v0, a virtual column: not pushed down
   * )
   * </pre>
   *
   * <p>The node scans get only {@code service_name == 'druid/broker'}, and return every column of the matching rows.
   * The resolved query then applies the whole filter, including {@code v0 == 'DRUID.HOST'}, to the gathered rows.</p>
   *
   * <p>Likewise, if the query defines a virtual column named {@code server}, a filter on {@code server} reads that
   * virtual column rather than the table's column, so it isn't pushed down. A filter on the root of a join of two
   * system tables isn't pushed into either table.</p>
   */
  @Nullable
  private static DimFilter getNodeScanFilter(
      final SystemTableDataSource dataSource,
      final SystemTableDescriptor descriptor,
      final Query<?> query
  )
  {
    // A filter on a join or another composite datasource can't be attributed to one of its system tables.
    if (!dataSource.equals(query.getDataSource()) || query.getFilter() == null) {
      return null;
    }

    final List<DimFilter> conjuncts = query.getFilter() instanceof AndDimFilter andFilter
                                      ? andFilter.getFields()
                                      : List.of(query.getFilter());
    final RowSignature tableSignature = descriptor.getRowSignature();
    final List<DimFilter> nodeConjuncts = new ArrayList<>();
    for (final DimFilter conjunct : conjuncts) {
      final Set<String> requiredColumns = conjunct.getRequiredColumns();
      if (requiredColumns.stream().allMatch(tableSignature::contains)
          && requiredColumns.stream().noneMatch(query.getVirtualColumns()::exists)) {
        nodeConjuncts.add(conjunct);
      }
    }
    if (nodeConjuncts.isEmpty()) {
      return null;
    }
    return nodeConjuncts.size() == 1 ? nodeConjuncts.getFirst() : new AndDimFilter(nodeConjuncts);
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
