/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.coordination;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Ordering;
import com.metamx.common.ISE;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import com.metamx.common.guava.YieldingSequenceBase;
import com.metamx.druid.Query;
import com.metamx.druid.StorageAdapter;
import com.metamx.druid.TimelineObjectHolder;
import com.metamx.druid.VersionedIntervalTimeline;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.collect.CountingMap;
import com.metamx.druid.index.QueryableIndex;
import com.metamx.druid.index.Segment;
import com.metamx.druid.loading.SegmentLoader;
import com.metamx.druid.loading.SegmentLoadingException;
import com.metamx.druid.partition.PartitionChunk;
import com.metamx.druid.partition.PartitionHolder;
import com.metamx.druid.query.BySegmentQueryRunner;
import com.metamx.druid.query.FinalizeResultsQueryRunner;
import com.metamx.druid.query.MetricsEmittingQueryRunner;
import com.metamx.druid.query.NoopQueryRunner;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryRunnerFactory;
import com.metamx.druid.query.QueryRunnerFactoryConglomerate;
import com.metamx.druid.query.QueryToolChest;
import com.metamx.druid.query.segment.QuerySegmentSpec;
import com.metamx.druid.query.segment.QuerySegmentWalker;
import com.metamx.druid.query.segment.SegmentDescriptor;
import com.metamx.druid.query.segment.SpecificSegmentQueryRunner;
import com.metamx.druid.query.segment.SpecificSegmentSpec;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class ServerManager implements QuerySegmentWalker
{
  private static final EmittingLogger log = new EmittingLogger(ServerManager.class);

  private final Object lock = new Object();

  private final SegmentLoader segmentLoader;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final ServiceEmitter emitter;
  private final ExecutorService exec;

  private final Map<String, VersionedIntervalTimeline<String, ReferenceCountingSegment>> dataSources;
  private final CountingMap<String> dataSourceSizes = new CountingMap<String>();
  private final CountingMap<String> dataSourceCounts = new CountingMap<String>();

  public ServerManager(
      SegmentLoader segmentLoader,
      QueryRunnerFactoryConglomerate conglomerate,
      ServiceEmitter emitter,
      ExecutorService exec
  )
  {
    this.segmentLoader = segmentLoader;
    this.conglomerate = conglomerate;
    this.emitter = emitter;

    this.exec = exec;

    this.dataSources = new HashMap<String, VersionedIntervalTimeline<String, ReferenceCountingSegment>>();
  }

  public Map<String, Long> getDataSourceSizes()
  {
    synchronized (dataSourceSizes) {
      return dataSourceSizes.snapshot();
    }
  }

  public Map<String, Long> getDataSourceCounts()
  {
    synchronized (dataSourceCounts) {
      return dataSourceCounts.snapshot();
    }
  }

  public boolean isSegmentCached(final DataSegment segment) throws SegmentLoadingException
  {
    return segmentLoader.isSegmentLoaded(segment);
  }

  public void loadSegment(final DataSegment segment) throws SegmentLoadingException
  {
    final Segment adapter;
    try {
      adapter = segmentLoader.getSegment(segment);
    }
    catch (SegmentLoadingException e) {
      try {
        segmentLoader.cleanup(segment);
      }
      catch (SegmentLoadingException e1) {
        // ignore
      }
      throw e;
    }

    if (adapter == null) {
      throw new SegmentLoadingException("Null adapter from loadSpec[%s]", segment.getLoadSpec());
    }

    synchronized (lock) {
      String dataSource = segment.getDataSource();
      VersionedIntervalTimeline<String, ReferenceCountingSegment> loadedIntervals = dataSources.get(dataSource);

      if (loadedIntervals == null) {
        loadedIntervals = new VersionedIntervalTimeline<String, ReferenceCountingSegment>(Ordering.natural());
        dataSources.put(dataSource, loadedIntervals);
      }

      PartitionHolder<ReferenceCountingSegment> entry = loadedIntervals.findEntry(
          segment.getInterval(),
          segment.getVersion()
      );
      if ((entry != null) && (entry.getChunk(segment.getShardSpec().getPartitionNum()) != null)) {
        log.info("Told to load a adapter for a segment[%s] that already exists", segment.getIdentifier());
        throw new SegmentLoadingException("Segment already exists[%s]", segment.getIdentifier());
      }

      loadedIntervals.add(
          segment.getInterval(),
          segment.getVersion(),
          segment.getShardSpec().createChunk(new ReferenceCountingSegment(adapter))
      );
      synchronized (dataSourceSizes) {
        dataSourceSizes.add(dataSource, segment.getSize());
      }
      synchronized (dataSourceCounts) {
        dataSourceCounts.add(dataSource, 1L);
      }
    }
  }

  public void dropSegment(final DataSegment segment) throws SegmentLoadingException
  {
    String dataSource = segment.getDataSource();
    synchronized (lock) {
      VersionedIntervalTimeline<String, ReferenceCountingSegment> loadedIntervals = dataSources.get(dataSource);

      if (loadedIntervals == null) {
        log.info("Told to delete a queryable for a dataSource[%s] that doesn't exist.", dataSource);
        return;
      }

      PartitionChunk<ReferenceCountingSegment> removed = loadedIntervals.remove(
          segment.getInterval(),
          segment.getVersion(),
          segment.getShardSpec().createChunk((ReferenceCountingSegment) null)
      );
      ReferenceCountingSegment oldQueryable = (removed == null) ? null : removed.getObject();

      if (oldQueryable != null) {
        synchronized (dataSourceSizes) {
          dataSourceSizes.add(dataSource, -segment.getSize());
        }
        synchronized (dataSourceCounts) {
          dataSourceCounts.add(dataSource, -1L);
        }

        oldQueryable.decrement();
      } else {
        log.info(
            "Told to delete a queryable on dataSource[%s] for interval[%s] and version [%s] that I don't have.",
            dataSource,
            segment.getInterval(),
            segment.getVersion()
        );
      }
    }
    segmentLoader.cleanup(segment);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      throw new ISE("Unknown query type[%s].", query.getClass());
    }

    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();

    final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline = dataSources.get(query.getDataSource());

    if (timeline == null) {
      return new NoopQueryRunner<T>();
    }

    FunctionalIterable<QueryRunner<T>> adapters = FunctionalIterable
        .create(intervals)
        .transformCat(
            new Function<Interval, Iterable<TimelineObjectHolder<String, ReferenceCountingSegment>>>()
            {
              @Override
              public Iterable<TimelineObjectHolder<String, ReferenceCountingSegment>> apply(Interval input)
              {
                return timeline.lookup(input);
              }
            }
        )
        .transformCat(
            new Function<TimelineObjectHolder<String, ReferenceCountingSegment>, Iterable<QueryRunner<T>>>()
            {
              @Override
              public Iterable<QueryRunner<T>> apply(
                  @Nullable final TimelineObjectHolder<String, ReferenceCountingSegment> holder
              )
              {
                if (holder == null) {
                  return null;
                }

                return FunctionalIterable
                    .create(holder.getObject())
                    .transform(
                        new Function<PartitionChunk<ReferenceCountingSegment>, QueryRunner<T>>()
                        {
                          @Override
                          public QueryRunner<T> apply(PartitionChunk<ReferenceCountingSegment> input)
                          {
                            return buildAndDecorateQueryRunner(
                                factory,
                                toolChest,
                                input.getObject(),
                                new SpecificSegmentSpec(
                                    new SegmentDescriptor(
                                        holder.getInterval(),
                                        holder.getVersion(),
                                        input.getChunkNumber()
                                    )
                                )
                            );
                          }
                        }
                    )
                    .filter(Predicates.<QueryRunner<T>>notNull());
              }
            }
        )
        .filter(
            Predicates.<QueryRunner<T>>notNull()
        );

    return new FinalizeResultsQueryRunner<T>(toolChest.mergeResults(factory.mergeRunners(exec, adapters)), toolChest);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      log.makeAlert("Unknown query type, [%s]", query.getClass())
         .addData("dataSource", query.getDataSource())
         .emit();
      return new NoopQueryRunner<T>();
    }

    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();

    final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline = dataSources.get(query.getDataSource());

    if (timeline == null) {
      return new NoopQueryRunner<T>();
    }

    FunctionalIterable<QueryRunner<T>> adapters = FunctionalIterable
        .create(specs)
        .transformCat(
            new Function<SegmentDescriptor, Iterable<QueryRunner<T>>>()
            {
              @Override
              @SuppressWarnings("unchecked")
              public Iterable<QueryRunner<T>> apply(SegmentDescriptor input)
              {
                final PartitionHolder<ReferenceCountingSegment> entry = timeline.findEntry(
                    input.getInterval(), input.getVersion()
                );

                if (entry == null) {
                  return null;
                }

                final PartitionChunk<ReferenceCountingSegment> chunk = entry.getChunk(input.getPartitionNumber());
                if (chunk == null) {
                  return null;
                }

                final ReferenceCountingSegment adapter = chunk.getObject();
                return Arrays.asList(
                    buildAndDecorateQueryRunner(factory, toolChest, adapter, new SpecificSegmentSpec(input))
                );
              }
            }
        )
        .filter(
            Predicates.<QueryRunner<T>>notNull()
        );

    return new FinalizeResultsQueryRunner<T>(toolChest.mergeResults(factory.mergeRunners(exec, adapters)), toolChest);
  }

  private <T> QueryRunner<T> buildAndDecorateQueryRunner(
      final QueryRunnerFactory<T, Query<T>> factory,
      final QueryToolChest<T, Query<T>> toolChest,
      final ReferenceCountingSegment adapter,
      final QuerySegmentSpec segmentSpec
  )
  {
    adapter.increment();

    return new SpecificSegmentQueryRunner<T>(
        new MetricsEmittingQueryRunner<T>(
            emitter,
            new Function<Query<T>, ServiceMetricEvent.Builder>()
            {
              @Override
              public ServiceMetricEvent.Builder apply(@Nullable final Query<T> input)
              {
                return toolChest.makeMetricBuilder(input);
              }
            },
            new BySegmentQueryRunner<T>(
                adapter.getIdentifier(),
                adapter.getDataInterval().getStart(),
                new QueryRunner<T>()
                {
                  @Override
                  public Sequence<T> run(final Query<T> query)
                  {
                    return new ReferenceCountingSequence<T>(factory.createRunner(adapter).run(query), adapter);
                  }
                }
            )
        ).withWaitMeasuredFromNow(),
        segmentSpec
    );
  }

  public static class ReferenceCountingSegment implements Segment
  {
    private final Segment baseSegment;

    private final AtomicInteger references = new AtomicInteger(0);

    public ReferenceCountingSegment(Segment baseSegment)
    {
      this.baseSegment = baseSegment;
    }

    @Override
    public String getIdentifier()
    {
      return baseSegment.getIdentifier();
    }

    @Override
    public Interval getDataInterval()
    {
      return baseSegment.getDataInterval();
    }

    @Override
    public QueryableIndex asQueryableIndex()
    {
      return baseSegment.asQueryableIndex();
    }

    @Override
    public StorageAdapter asStorageAdapter()
    {
      return baseSegment.asStorageAdapter();
    }

    @Override
    public void close() throws IOException
    {
      baseSegment.close();
    }

    public void increment()
    {
      references.getAndIncrement();
    }

    public void decrement()
    {
      references.getAndDecrement();

      if (references.get() < 0) {
        try {
          close();
        }
        catch (Exception e) {
          log.error("Unable to close queryable index %s", getIdentifier());
        }
      }
    }
  }

  private static class ReferenceCountingSequence<T> extends YieldingSequenceBase<T>
  {
    private final Sequence<T> baseSequence;
    private final ReferenceCountingSegment segment;

    public ReferenceCountingSequence(Sequence<T> baseSequence, ReferenceCountingSegment segment)
    {
      this.baseSequence = baseSequence;
      this.segment = segment;
    }

    @Override
    public <OutType> Yielder<OutType> toYielder(
        OutType initValue, YieldingAccumulator<OutType, T> accumulator
    )
    {
      return new ReferenceCountingYielder<OutType>(baseSequence.toYielder(initValue, accumulator), segment);
    }
  }

  private static class ReferenceCountingYielder<OutType> implements Yielder<OutType>
  {
    private final Yielder<OutType> baseYielder;
    private final ReferenceCountingSegment segment;

    public ReferenceCountingYielder(Yielder<OutType> baseYielder, ReferenceCountingSegment segment)
    {
      this.baseYielder = baseYielder;
      this.segment = segment;
    }

    @Override
    public OutType get()
    {
      return baseYielder.get();
    }

    @Override
    public Yielder<OutType> next(OutType initValue)
    {
      return new ReferenceCountingYielder<OutType>(baseYielder.next(initValue), segment);
    }

    @Override
    public boolean isDone()
    {
      return baseYielder.isDone();
    }

    @Override
    public void close() throws IOException
    {
      segment.decrement();
      baseYielder.close();
    }
  }
}
