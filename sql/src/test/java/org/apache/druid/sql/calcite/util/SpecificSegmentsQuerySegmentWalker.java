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

package org.apache.druid.sql.calcite.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.io.Closeables;
import org.apache.druid.client.SegmentServerSelector;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.FunctionalIterable;
import org.apache.druid.java.util.common.guava.LazySequence;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.NoopQueryRunner;
import org.apache.druid.query.Queries;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.RetryQueryRunnerConfig;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.spec.SpecificSegmentQueryRunner;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.MapSegmentWrangler;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.join.InlineJoinableFactory;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.Joinables;
import org.apache.druid.segment.join.LookupJoinableFactory;
import org.apache.druid.segment.join.MapJoinableFactoryTest;
import org.apache.druid.server.ClientQuerySegmentWalker;
import org.apache.druid.server.LocalQuerySegmentWalker;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * A self-contained class that executes queries similarly to the normal Druid query stack.
 *
 * {@link ClientQuerySegmentWalker}, the same class that Brokers use as the entry point for their query stack, is
 * used directly. Our own class {@link DataServerLikeWalker} mimics the behavior of
 * {@link org.apache.druid.server.coordination.ServerManager}, the entry point for Historicals. That class isn't used
 * directly because the sheer volume of dependencies makes it quite verbose to use in a test environment.
 */
public class SpecificSegmentsQuerySegmentWalker implements QuerySegmentWalker, Closeable
{
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final QuerySegmentWalker walker;
  private final JoinableFactory joinableFactory;
  private final QueryScheduler scheduler;
  private final Map<String, VersionedIntervalTimeline<String, ReferenceCountingSegment>> timelines = new HashMap<>();
  private final List<Closeable> closeables = new ArrayList<>();
  private final List<DataSegment> segments = new ArrayList<>();

  /**
   * Create an instance using the provided query runner factory conglomerate and lookup provider.
   * If a JoinableFactory is provided, it will be used instead of the default. If a scheduler is included,
   * the runner will schedule queries according to the scheduling config.
   */
  public SpecificSegmentsQuerySegmentWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      final LookupExtractorFactoryContainerProvider lookupProvider,
      @Nullable final JoinableFactory joinableFactory,
      @Nullable final QueryScheduler scheduler
  )
  {
    final NoopServiceEmitter emitter = new NoopServiceEmitter();

    this.conglomerate = conglomerate;
    this.joinableFactory = joinableFactory == null ?
                           MapJoinableFactoryTest.fromMap(
                               ImmutableMap.<Class<? extends DataSource>, JoinableFactory>builder()
                                   .put(InlineDataSource.class, new InlineJoinableFactory())
                                   .put(LookupDataSource.class, new LookupJoinableFactory(lookupProvider))
                                   .build()
                           ) : joinableFactory;

    this.scheduler = scheduler;
    this.walker = new ClientQuerySegmentWalker(
        emitter,
        new DataServerLikeWalker(),
        new LocalQuerySegmentWalker(
            conglomerate,
            new MapSegmentWrangler(ImmutableMap.of()),
            this.joinableFactory,
            emitter
        ),
        new QueryToolChestWarehouse()
        {
          @Override
          public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(final QueryType query)
          {
            return conglomerate.findFactory(query).getToolchest();
          }
        },
        new RetryQueryRunnerConfig(),
        TestHelper.makeJsonMapper(),
        new ServerConfig(),
        null /* Cache */,
        new CacheConfig()
        {
          @Override
          public boolean isPopulateCache()
          {
            return false;
          }

          @Override
          public boolean isUseCache()
          {
            return false;
          }

          @Override
          public boolean isPopulateResultLevelCache()
          {
            return false;
          }

          @Override
          public boolean isUseResultLevelCache()
          {
            return false;
          }
        }
    );
  }

  /**
   * Create an instance using the provided query runner factory conglomerate and lookup provider.
   * If a JoinableFactory is provided, it will be used instead of the default.
   */
  public SpecificSegmentsQuerySegmentWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      final LookupExtractorFactoryContainerProvider lookupProvider,
      @Nullable final JoinableFactory joinableFactory
  )
  {
    this(conglomerate, lookupProvider, joinableFactory, null);
  }


  /**
   * Create an instance without any lookups, using the default JoinableFactory
   */
  public SpecificSegmentsQuerySegmentWalker(final QueryRunnerFactoryConglomerate conglomerate)
  {
    this(conglomerate, null);
  }

  /**
   * Create an instance without any lookups, optionally allowing the default JoinableFactory to be overridden
   */
  public SpecificSegmentsQuerySegmentWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      @Nullable JoinableFactory joinableFactory
  )
  {
    this(
        conglomerate,
        new LookupExtractorFactoryContainerProvider()
        {
          @Override
          public Set<String> getAllLookupNames()
          {
            return Collections.emptySet();
          }

          @Override
          public Optional<LookupExtractorFactoryContainer> get(String lookupName)
          {
            return Optional.empty();
          }
        },
        joinableFactory
    );
  }

  public SpecificSegmentsQuerySegmentWalker add(
      final DataSegment descriptor,
      final QueryableIndex index
  )
  {
    final Segment segment = new QueryableIndexSegment(index, descriptor.getId());
    final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline = timelines.computeIfAbsent(
        descriptor.getDataSource(),
        datasource -> new VersionedIntervalTimeline<>(Ordering.natural())
    );
    timeline.add(
        descriptor.getInterval(),
        descriptor.getVersion(),
        descriptor.getShardSpec().createChunk(ReferenceCountingSegment.wrapSegment(segment, descriptor.getShardSpec()))
    );
    segments.add(descriptor);
    closeables.add(index);
    return this;
  }

  public List<DataSegment> getSegments()
  {
    return segments;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(
      final Query<T> query,
      final Iterable<Interval> intervals
  )
  {
    return walker.getQueryRunnerForIntervals(query, intervals);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(
      final Query<T> query,
      final Iterable<SegmentDescriptor> specs
  )
  {
    return walker.getQueryRunnerForSegments(query, specs);
  }

  @Override
  public void close() throws IOException
  {
    for (Closeable closeable : closeables) {
      Closeables.close(closeable, true);
    }
  }

  private List<WindowedSegment> getSegmentsForTable(final String dataSource, final Interval interval)
  {
    final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline = timelines.get(dataSource);

    if (timeline == null) {
      return Collections.emptyList();
    } else {
      final List<WindowedSegment> retVal = new ArrayList<>();

      for (TimelineObjectHolder<String, ReferenceCountingSegment> holder : timeline.lookup(interval)) {
        for (PartitionChunk<ReferenceCountingSegment> chunk : holder.getObject()) {
          retVal.add(new WindowedSegment(chunk.getObject(), holder.getInterval()));
        }
      }

      return retVal;
    }
  }

  private List<WindowedSegment> getSegmentsForTable(final String dataSource, final Iterable<SegmentDescriptor> specs)
  {
    final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline = timelines.get(dataSource);

    if (timeline == null) {
      return Collections.emptyList();
    } else {
      final List<WindowedSegment> retVal = new ArrayList<>();

      for (SegmentDescriptor spec : specs) {
        final PartitionHolder<ReferenceCountingSegment> entry = timeline.findEntry(
            spec.getInterval(),
            spec.getVersion()
        );
        retVal.add(new WindowedSegment(entry.getChunk(spec.getPartitionNumber()).getObject(), spec.getInterval()));
      }

      return retVal;
    }
  }

  public static class WindowedSegment
  {
    private final Segment segment;
    private final Interval interval;

    public WindowedSegment(Segment segment, Interval interval)
    {
      this.segment = segment;
      this.interval = interval;
      Preconditions.checkArgument(segment.getId().getInterval().contains(interval));
    }

    public Segment getSegment()
    {
      return segment;
    }

    public Interval getInterval()
    {
      return interval;
    }

    public SegmentDescriptor getDescriptor()
    {
      return new SegmentDescriptor(interval, segment.getId().getVersion(), segment.getId().getPartitionNum());
    }
  }

  /**
   * Mimics the behavior of a data server (e.g. Historical).
   *
   * Compare to {@link org.apache.druid.server.SegmentManager}.
   */
  private class DataServerLikeWalker implements QuerySegmentWalker
  {
    @Override
    public <T> QueryRunner<T> getQueryRunnerForIntervals(final Query<T> query, final Iterable<Interval> intervals)
    {
      final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(query.getDataSource());

      if (!analysis.isConcreteTableBased()) {
        throw new ISE("Cannot handle datasource: %s", query.getDataSource());
      }

      final String dataSourceName = ((TableDataSource) analysis.getBaseDataSource()).getName();

      FunctionalIterable<SegmentDescriptor> segmentDescriptors = FunctionalIterable
          .create(intervals)
          .transformCat(interval -> getSegmentsForTable(dataSourceName, interval))
          .transform(WindowedSegment::getDescriptor);

      return getQueryRunnerForSegments(query, segmentDescriptors);
    }

    @Override
    public <T> QueryRunner<T> getQueryRunnerForSegments(final Query<T> query, final Iterable<SegmentDescriptor> specs)
    {
      final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
      if (factory == null) {
        throw new ISE("Unknown query type[%s].", query.getClass());
      }

      final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(query.getDataSource());

      if (!analysis.isConcreteTableBased()) {
        throw new ISE("Cannot handle datasource: %s", query.getDataSource());
      }

      final String dataSourceName = ((TableDataSource) analysis.getBaseDataSource()).getName();

      final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();

      // Make sure this query type can handle the subquery, if present.
      if (analysis.isQuery()
          && !toolChest.canPerformSubquery(((QueryDataSource) analysis.getDataSource()).getQuery())) {
        throw new ISE("Cannot handle subquery: %s", analysis.getDataSource());
      }

      final Function<Segment, Segment> segmentMapFn = Joinables.createSegmentMapFn(
          analysis.getPreJoinableClauses(),
          joinableFactory,
          new AtomicLong(),
          QueryContexts.getEnableJoinFilterPushDown(query),
          QueryContexts.getEnableJoinFilterRewrite(query),
          QueryContexts.getEnableJoinFilterRewriteValueColumnFilters(query),
          QueryContexts.getJoinFilterRewriteMaxSize(query),
          query.getFilter() == null ? null : query.getFilter().toFilter(),
          query.getVirtualColumns()
      );

      final QueryRunner<T> baseRunner = new FinalizeResultsQueryRunner<>(
          toolChest.postMergeQueryDecoration(
              toolChest.mergeResults(
                  toolChest.preMergeQueryDecoration(
                      makeTableRunner(toolChest, factory, getSegmentsForTable(dataSourceName, specs), segmentMapFn)
                  )
              )
          ),
          toolChest
      );


      // Wrap baseRunner in a runner that rewrites the QuerySegmentSpec to mention the specific segments.
      // This mimics what CachingClusteredClient on the Broker does, and is required for certain queries (like Scan)
      // to function properly.
      return (theQuery, responseContext) -> {
        if (scheduler != null) {
          Set<SegmentServerSelector> segments = new HashSet<>();
          specs.forEach(spec -> segments.add(new SegmentServerSelector(null, spec)));
          return scheduler.run(
              scheduler.prioritizeAndLaneQuery(theQuery, segments),
              new LazySequence<>(
                  () -> baseRunner.run(
                      theQuery.withQuery(Queries.withSpecificSegments(
                          theQuery.getQuery(),
                          ImmutableList.copyOf(specs)
                      )),
                      responseContext
                  )
              )
          );
        } else {
          return baseRunner.run(
              theQuery.withQuery(Queries.withSpecificSegments(theQuery.getQuery(), ImmutableList.copyOf(specs))),
              responseContext
          );
        }
      };
    }

    private <T> QueryRunner<T> makeTableRunner(
        final QueryToolChest<T, Query<T>> toolChest,
        final QueryRunnerFactory<T, Query<T>> factory,
        final Iterable<WindowedSegment> segments,
        final Function<Segment, Segment> segmentMapFn
    )
    {
      final List<WindowedSegment> segmentsList = Lists.newArrayList(segments);

      if (segmentsList.isEmpty()) {
        // Note: this is not correct when there's a right or full outer join going on.
        // See https://github.com/apache/druid/issues/9229 for details.
        return new NoopQueryRunner<>();
      }

      return new FinalizeResultsQueryRunner<>(
          toolChest.mergeResults(
              factory.mergeRunners(
                  Execs.directExecutor(),
                  FunctionalIterable
                      .create(segmentsList)
                      .transform(
                          segment ->
                              new SpecificSegmentQueryRunner<>(
                                  factory.createRunner(segmentMapFn.apply(segment.getSegment())),
                                  new SpecificSegmentSpec(segment.getDescriptor())
                              )
                      )
              )
          ),
          toolChest
      );
    }
  }
}
