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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.io.Closeables;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.FunctionalIterable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.NoopQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.query.spec.SpecificSegmentQueryRunner;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SpecificSegmentsQuerySegmentWalker implements QuerySegmentWalker, Closeable
{
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final Map<String, VersionedIntervalTimeline<String, ReferenceCountingSegment>> timelines = new HashMap<>();
  private final List<Closeable> closeables = new ArrayList<>();
  private final List<DataSegment> segments = new ArrayList<>();

  public SpecificSegmentsQuerySegmentWalker(QueryRunnerFactoryConglomerate conglomerate)
  {
    this.conglomerate = conglomerate;
  }

  public SpecificSegmentsQuerySegmentWalker add(
      final DataSegment descriptor,
      final QueryableIndex index
  )
  {
    final Segment segment = new QueryableIndexSegment(index, descriptor.getId());
    final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline = timelines
        .computeIfAbsent(descriptor.getDataSource(), datasource -> new VersionedIntervalTimeline<>(Ordering.natural()));
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
    Query<T> newQuery = query;
    if (query instanceof ScanQuery && ((ScanQuery) query).getOrder() != ScanQuery.Order.NONE) {
      newQuery = (Query<T>) Druids.ScanQueryBuilder.copy((ScanQuery) query)
                                                   .intervals(new MultipleSpecificSegmentSpec(ImmutableList.of()))
                                                   .build();
    }

    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(newQuery);
    if (factory == null) {
      throw new ISE("Unknown query type[%s].", newQuery.getClass());
    }

    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();

    return new FinalizeResultsQueryRunner<>(
        toolChest.postMergeQueryDecoration(
            toolChest.mergeResults(
                toolChest.preMergeQueryDecoration(
                    (queryPlus, responseContext) -> {
                      Query<T> query1 = queryPlus.getQuery();
                      Query<T> newQuery1 = query1;
                      if (query instanceof ScanQuery && ((ScanQuery) query).getOrder() != ScanQuery.Order.NONE) {
                        newQuery1 = (Query<T>) Druids.ScanQueryBuilder.copy((ScanQuery) query)
                                                                      .intervals(new MultipleSpecificSegmentSpec(
                                                                          ImmutableList.of(new SegmentDescriptor(
                                                                              Intervals.of("2015-04-12/2015-04-13"),
                                                                              "4",
                                                                              0
                                                                          ))))
                                                                      .context(ImmutableMap.of(
                                                                          ScanQuery.CTX_KEY_OUTERMOST,
                                                                          false
                                                                      ))
                                                                      .build();
                      }
                      final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline = getTimelineForTableDataSource(
                          newQuery1);
                      return makeBaseRunner(
                          newQuery1,
                          toolChest,
                          factory,
                          FunctionalIterable
                              .create(intervals)
                              .transformCat(
                                  interval -> timeline.lookup(interval)
                              )
                              .transformCat(
                                  holder -> FunctionalIterable
                                      .create(holder.getObject())
                                      .transform(
                                          chunk -> new SegmentDescriptor(
                                              holder.getInterval(),
                                              holder.getVersion(),
                                              chunk.getChunkNumber()
                                          )
                                      )
                              )
                      ).run(QueryPlus.wrap(newQuery1), responseContext);
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

  private <T> VersionedIntervalTimeline<String, ReferenceCountingSegment> getTimelineForTableDataSource(Query<T> query)
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
    final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline = getTimelineForTableDataSource(query);
    if (timeline == null) {
      return new NoopQueryRunner<>();
    }

    return new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            factory.mergeRunners(
                Execs.directExecutor(),
                FunctionalIterable
                    .create(specs)
                    .transformCat(
                        descriptor -> {
                          final PartitionHolder<ReferenceCountingSegment> holder = timeline.findEntry(
                              descriptor.getInterval(),
                              descriptor.getVersion()
                          );

                          return Iterables.transform(
                              holder,
                              chunk -> new SpecificSegmentQueryRunner<T>(
                                  factory.createRunner(chunk.getObject()),
                                  new SpecificSegmentSpec(descriptor)
                              )
                          );
                        }
                    )
            )
        ),
        toolChest
    );
  }
}
