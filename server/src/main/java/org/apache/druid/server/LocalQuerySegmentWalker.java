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

import com.google.inject.Inject;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.FunctionalIterable;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.FluentQueryRunnerBuilder;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.SegmentWrangler;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.Joinables;
import org.joda.time.Interval;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.StreamSupport;

/**
 * Processor that computes Druid queries, single-threaded.
 *
 * The datasource for the query must satisfy {@link DataSourceAnalysis#isConcreteBased()} and
 * {@link DataSourceAnalysis#isGlobal()}. Its base datasource must also be handleable by the provided
 * {@link SegmentWrangler}.
 *
 * Mainly designed to be used by {@link ClientQuerySegmentWalker}.
 */
public class LocalQuerySegmentWalker implements QuerySegmentWalker
{
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final SegmentWrangler segmentWrangler;
  private final JoinableFactory joinableFactory;
  private final QueryScheduler scheduler;
  private final ServiceEmitter emitter;

  @Inject
  public LocalQuerySegmentWalker(
      QueryRunnerFactoryConglomerate conglomerate,
      SegmentWrangler segmentWrangler,
      JoinableFactory joinableFactory,
      QueryScheduler scheduler,
      ServiceEmitter emitter
  )
  {
    this.conglomerate = conglomerate;
    this.segmentWrangler = segmentWrangler;
    this.joinableFactory = joinableFactory;
    this.scheduler = scheduler;
    this.emitter = emitter;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(final Query<T> query, final Iterable<Interval> intervals)
  {
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(query.getDataSource());

    if (!analysis.isConcreteBased() || !analysis.isGlobal()) {
      throw new IAE("Cannot query dataSource locally: %s", analysis.getDataSource());
    }

    // wrap in ReferenceCountingSegment, these aren't currently managed by SegmentManager so reference tracking doesn't
    // matter, but at least some or all will be in a future PR
    final Iterable<ReferenceCountingSegment> segments =
        FunctionalIterable.create(segmentWrangler.getSegmentsForIntervals(analysis.getBaseDataSource(), intervals))
                          .transform(ReferenceCountingSegment::wrapRootGenerationSegment);

    final AtomicLong cpuAccumulator = new AtomicLong(0L);

    final Function<SegmentReference, SegmentReference> segmentMapFn = Joinables.createSegmentMapFn(
        analysis.getPreJoinableClauses(),
        joinableFactory,
        cpuAccumulator,
        analysis.getBaseQuery().orElse(query)
    );

    final QueryRunnerFactory<T, Query<T>> queryRunnerFactory = conglomerate.findFactory(query);
    final QueryRunner<T> baseRunner = queryRunnerFactory.mergeRunners(
        Execs.directExecutor(),
        () -> StreamSupport.stream(segments.spliterator(), false)
                           .map(segmentMapFn)
                           .map(queryRunnerFactory::createRunner).iterator()
    );

    // Note: Not calling 'postProcess'; it isn't official/documented functionality so we'll only support it where
    // it is already supported.
    return new FluentQueryRunnerBuilder<>(queryRunnerFactory.getToolchest())
        .create(scheduler.wrapQueryRunner(baseRunner))
        .applyPreMergeDecoration()
        .mergeResults()
        .applyPostMergeDecoration()
        .emitCPUTimeMetric(emitter, cpuAccumulator);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(final Query<T> query, final Iterable<SegmentDescriptor> specs)
  {
    // SegmentWranglers only work based on intervals and cannot run with specific segments.
    throw new ISE("Cannot run with specific segments");
  }
}
