/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.util;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.UOE;
import io.druid.java.util.common.guava.FunctionalIterable;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.SegmentDescriptor;
import io.druid.query.TableDataSource;
import io.druid.query.spec.SpecificSegmentQueryRunner;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SpecificSegmentsQuerySegmentWalker implements QuerySegmentWalker, Closeable
{
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final Map<String, VersionedIntervalTimeline<String, Segment>> timelines = Maps.newHashMap();
  private final List<Closeable> closeables = Lists.newArrayList();
  private final List<DataSegment> segments = Lists.newArrayList();

  public SpecificSegmentsQuerySegmentWalker(QueryRunnerFactoryConglomerate conglomerate)
  {
    this.conglomerate = conglomerate;
  }

  public SpecificSegmentsQuerySegmentWalker add(
      final DataSegment descriptor,
      final QueryableIndex index
  )
  {
    final Segment segment = new QueryableIndexSegment(descriptor.getIdentifier(), index);
    if (!timelines.containsKey(descriptor.getDataSource())) {
      timelines.put(descriptor.getDataSource(), new VersionedIntervalTimeline<String, Segment>(Ordering.natural()));
    }

    final VersionedIntervalTimeline<String, Segment> timeline = timelines.get(descriptor.getDataSource());
    timeline.add(descriptor.getInterval(), descriptor.getVersion(), descriptor.getShardSpec().createChunk(segment));
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
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      throw new ISE("Unknown query type[%s].", query.getClass());
    }

    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();

    return new FinalizeResultsQueryRunner<>(
        toolChest.postMergeQueryDecoration(
            toolChest.mergeResults(
                toolChest.preMergeQueryDecoration(
                    new QueryRunner<T>()
                    {
                      @Override
                      public Sequence<T> run(QueryPlus<T> queryPlus, Map<String, Object> responseContext)
                      {
                        Query<T> query = queryPlus.getQuery();
                        final VersionedIntervalTimeline<String, Segment> timeline = getTimelineForTableDataSource(query);
                        return makeBaseRunner(
                            query,
                            toolChest,
                            factory,
                            FunctionalIterable
                                .create(intervals)
                                .transformCat(
                                    new Function<Interval, Iterable<TimelineObjectHolder<String, Segment>>>()
                                    {
                                      @Override
                                      public Iterable<TimelineObjectHolder<String, Segment>> apply(final Interval interval)
                                      {
                                        return timeline.lookup(interval);
                                      }
                                    }
                                )
                                .transformCat(
                                    new Function<TimelineObjectHolder<String, Segment>, Iterable<SegmentDescriptor>>()
                                    {
                                      @Override
                                      public Iterable<SegmentDescriptor> apply(final TimelineObjectHolder<String, Segment> holder)
                                      {
                                        return FunctionalIterable
                                            .create(holder.getObject())
                                            .transform(
                                                new Function<PartitionChunk<Segment>, SegmentDescriptor>()
                                                {
                                                  @Override
                                                  public SegmentDescriptor apply(final PartitionChunk<Segment> chunk)
                                                  {
                                                    return new SegmentDescriptor(
                                                        holder.getInterval(),
                                                        holder.getVersion(),
                                                        chunk.getChunkNumber()
                                                    );
                                                  }
                                                }
                                            );
                                      }
                                    }
                                )
                        ).run(queryPlus, responseContext);
                      }
                    }
                )
            )
        ),
        toolChest
    );
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(
      final Query<T> query,
      final Iterable<SegmentDescriptor> specs
  )
  {
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      throw new ISE("Unknown query type[%s].", query.getClass());
    }

    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();

    return new FinalizeResultsQueryRunner<>(
        toolChest.postMergeQueryDecoration(
            toolChest.mergeResults(
                toolChest.preMergeQueryDecoration(
                    makeBaseRunner(query, toolChest, factory, specs)
                )
            )
        ),
        toolChest
    );
  }

  @Override
  public void close() throws IOException
  {
    for (Closeable closeable : closeables) {
      Closeables.close(closeable, true);
    }
  }

  private <T> VersionedIntervalTimeline<String, Segment> getTimelineForTableDataSource(Query<T> query)
  {
    if (query.getDataSource() instanceof TableDataSource) {
      return timelines.get(((TableDataSource) query.getDataSource()).getName());
    } else {
      throw new UOE("DataSource type[%s] unsupported", query.getDataSource().getClass().getName());
    }
  }

  private <T> QueryRunner<T> makeBaseRunner(
      final Query<T> query,
      final QueryToolChest<T, Query<T>> toolChest,
      final QueryRunnerFactory<T, Query<T>> factory,
      final Iterable<SegmentDescriptor> specs
  )
  {
    final VersionedIntervalTimeline<String, Segment> timeline = getTimelineForTableDataSource(query);
    if (timeline == null) {
      return new NoopQueryRunner<>();
    }

    return new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            factory.mergeRunners(
                MoreExecutors.sameThreadExecutor(),
                FunctionalIterable
                    .create(specs)
                    .transformCat(
                        new Function<SegmentDescriptor, Iterable<QueryRunner<T>>>()
                        {
                          @Override
                          public Iterable<QueryRunner<T>> apply(final SegmentDescriptor descriptor)
                          {
                            final PartitionHolder<Segment> holder = timeline.findEntry(
                                descriptor.getInterval(),
                                descriptor.getVersion()
                            );

                            return Iterables.transform(
                                holder,
                                new Function<PartitionChunk<Segment>, QueryRunner<T>>()
                                {
                                  @Override
                                  public QueryRunner<T> apply(PartitionChunk<Segment> chunk)
                                  {
                                    return new SpecificSegmentQueryRunner<T>(
                                        factory.createRunner(chunk.getObject()),
                                        new SpecificSegmentSpec(descriptor)
                                    );
                                  }
                                }
                            );
                          }
                        }
                    )
            )
        ),
        toolChest
    );
  }
}
