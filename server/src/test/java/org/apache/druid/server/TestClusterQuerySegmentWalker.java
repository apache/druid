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

package org.apache.druid.server;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.client.SegmentServerSelector;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.FunctionalIterable;
import org.apache.druid.java.util.common.guava.LazySequence;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.NoopQueryRunner;
import org.apache.druid.query.Queries;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.context.ResponseContext.Key;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.spec.SpecificSegmentQueryRunner;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.Joinables;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Mimics the behavior of {@link org.apache.druid.client.CachingClusteredClient} when it queries data servers (like
 * Historicals, which use {@link org.apache.druid.server.coordination.ServerManager}). Used by {@link QueryStackTests}.
 *
 * This class's logic is like a mashup of those two classes. With the right abstractions, it may be possible to get rid
 * of this class and replace it with the production classes.
 */
public class TestClusterQuerySegmentWalker implements QuerySegmentWalker
{
  private final Map<String, VersionedIntervalTimeline<String, ReferenceCountingSegment>> timelines;
  private final JoinableFactory joinableFactory;
  private final QueryRunnerFactoryConglomerate conglomerate;
  @Nullable
  private final QueryScheduler scheduler;

  TestClusterQuerySegmentWalker(
      Map<String, VersionedIntervalTimeline<String, ReferenceCountingSegment>> timelines,
      JoinableFactory joinableFactory,
      QueryRunnerFactoryConglomerate conglomerate,
      @Nullable QueryScheduler scheduler
  )
  {
    this.timelines = timelines;
    this.joinableFactory = joinableFactory;
    this.conglomerate = conglomerate;
    this.scheduler = scheduler;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(final Query<T> query, final Iterable<Interval> intervals)
  {
    // Just like CachingClusteredClient, ignore "query" and defer action until the QueryRunner is called.
    // Strange, but true. Required to get authentic behavior with UnionDataSources. (Although, it would be great if
    // this wasn't required.)
    return (queryPlus, responseContext) -> {
      final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(queryPlus.getQuery().getDataSource());

      if (!analysis.isConcreteTableBased()) {
        throw new ISE("Cannot handle datasource: %s", queryPlus.getQuery().getDataSource());
      }

      final String dataSourceName = ((TableDataSource) analysis.getBaseDataSource()).getName();

      FunctionalIterable<SegmentDescriptor> segmentDescriptors = FunctionalIterable
          .create(intervals)
          .transformCat(interval -> getSegmentsForTable(dataSourceName, interval))
          .transform(WindowedSegment::getDescriptor);

      return getQueryRunnerForSegments(queryPlus.getQuery(), segmentDescriptors).run(queryPlus, responseContext);
    };
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

    final Function<SegmentReference, SegmentReference> segmentMapFn = Joinables.createSegmentMapFn(
        analysis.getPreJoinableClauses(),
        joinableFactory,
        new AtomicLong(),
        analysis.getBaseQuery().orElse(query)
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
    // to function properly. SegmentServerSelector does not currently mimic CachingClusteredClient, it is using
    // the LocalQuerySegmentWalker constructor instead since this walker is not mimic remote DruidServer objects
    // to actually serve the queries
    return (theQuery, responseContext) -> {
      responseContext.put(Key.REMAINING_RESPONSES_FROM_QUERY_SERVERS, new ConcurrentHashMap<>());
      responseContext.add(
          Key.REMAINING_RESPONSES_FROM_QUERY_SERVERS,
          new NonnullPair<>(theQuery.getQuery().getMostSpecificId(), 0)
      );
      if (scheduler != null) {
        Set<SegmentServerSelector> segments = new HashSet<>();
        specs.forEach(spec -> segments.add(new SegmentServerSelector(spec)));
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
      final Function<SegmentReference, SegmentReference> segmentMapFn
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
                                factory.createRunner(segmentMapFn.apply(ReferenceCountingSegment.wrapRootGenerationSegment(
                                    segment.getSegment()))),
                                new SpecificSegmentSpec(segment.getDescriptor())
                            )
                    )
            )
        ),
        toolChest
    );
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

  private static class WindowedSegment
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
}
