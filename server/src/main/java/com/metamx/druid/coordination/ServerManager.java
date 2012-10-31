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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;

import org.joda.time.Interval;

import com.google.common.base.Function;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.druid.Query;
import com.metamx.druid.StorageAdapter;
import com.metamx.druid.TimelineObjectHolder;
import com.metamx.druid.VersionedIntervalTimeline;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.collect.CountingMap;
import com.metamx.druid.index.v1.SegmentIdAttachedStorageAdapter;
import com.metamx.druid.loading.StorageAdapterLoader;
import com.metamx.druid.loading.StorageAdapterLoadingException;
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

/**
 */
public class ServerManager implements QuerySegmentWalker
{
  private static final EmittingLogger log = new EmittingLogger(ServerManager.class);

  private final Object lock = new Object();

  private final StorageAdapterLoader storageAdapterLoader;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final ServiceEmitter emitter;
  private final ExecutorService exec;

  private final Map<String, VersionedIntervalTimeline<String, StorageAdapter>> dataSources;
  private final CountingMap<String> dataSourceSizes = new CountingMap<String>();
  private final CountingMap<String> dataSourceCounts = new CountingMap<String>();

  public ServerManager(
      StorageAdapterLoader storageAdapterLoader,
      QueryRunnerFactoryConglomerate conglomerate,
      ServiceEmitter emitter,
      ExecutorService exec
  )
  {
    this.storageAdapterLoader = storageAdapterLoader;
    this.conglomerate = conglomerate;
    this.emitter = emitter;

    this.exec = exec;

    this.dataSources = new HashMap<String, VersionedIntervalTimeline<String, StorageAdapter>>();
  }

  public Map<String, Long> getDataSourceSizes()
  {
    return dataSourceSizes.snapshot();
  }

  public Map<String, Long> getDataSourceCounts()
  {
    return dataSourceCounts.snapshot();
  }

  public void loadSegment(final DataSegment segment) throws StorageAdapterLoadingException
  {
    StorageAdapter adapter = null;
    try {
      adapter = storageAdapterLoader.getAdapter(segment.getLoadSpec());
    }
    catch (StorageAdapterLoadingException e) {
      storageAdapterLoader.cleanupAdapter(segment.getLoadSpec());
      throw e;
    }

    if (adapter == null) {
      throw new StorageAdapterLoadingException("Null adapter from loadSpec[%s]", segment.getLoadSpec());
    }

    adapter = new SegmentIdAttachedStorageAdapter(segment.getIdentifier(), adapter);

    synchronized (lock) {
      String dataSource = segment.getDataSource();
      VersionedIntervalTimeline<String, StorageAdapter> loadedIntervals = dataSources.get(dataSource);

      if (loadedIntervals == null) {
        loadedIntervals = new VersionedIntervalTimeline<String, StorageAdapter>(Ordering.natural());
        dataSources.put(dataSource, loadedIntervals);
      }

      PartitionHolder<StorageAdapter> entry = loadedIntervals.findEntry(
          segment.getInterval(),
          segment.getVersion()
      );
      if ((entry != null) && (entry.getChunk(segment.getShardSpec().getPartitionNum()) != null)) {
        log.info("Told to load a adapter for a segment[%s] that already exists", segment.getIdentifier());
        throw new StorageAdapterLoadingException("Segment already exists[%s]", segment.getIdentifier());
      }

      loadedIntervals.add(
          segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(adapter)
      );
      dataSourceSizes.add(dataSource, segment.getSize());
      dataSourceCounts.add(dataSource, 1L);
    }
  }

  public void dropSegment(final DataSegment segment) throws StorageAdapterLoadingException
  {
    String dataSource = segment.getDataSource();
    synchronized (lock) {
      VersionedIntervalTimeline<String, StorageAdapter> loadedIntervals = dataSources.get(dataSource);

      if (loadedIntervals == null) {
        log.info("Told to delete a queryable for a dataSource[%s] that doesn't exist.", dataSource);
        return;
      }

      PartitionChunk<StorageAdapter> removed = loadedIntervals.remove(
          segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk((StorageAdapter) null)
      );
      StorageAdapter oldQueryable = (removed == null) ? null : removed.getObject();

      if (oldQueryable != null) {
        dataSourceSizes.add(dataSource, -segment.getSize());
        dataSourceCounts.add(dataSource, -1L);
      } else {
        log.info(
            "Told to delete a queryable on dataSource[%s] for interval[%s] and version [%s] that I don't have.",
            dataSource,
            segment.getInterval(),
            segment.getVersion()
        );
      }
    }
    storageAdapterLoader.cleanupAdapter(segment.getLoadSpec());
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      log.makeAlert("Unknown query type, [%s]", query.getClass())
         .addData("dataSource", query.getDataSource())
         .emit();
      return new NoopQueryRunner<T>();
    }

    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();

    final VersionedIntervalTimeline<String, StorageAdapter> timeline = dataSources.get(query.getDataSource());

    if (timeline == null) {
      return new NoopQueryRunner<T>();
    }

    FunctionalIterable<QueryRunner<T>> adapters = FunctionalIterable
        .create(intervals)
        .transformCat(
            new Function<Interval, Iterable<TimelineObjectHolder<String, StorageAdapter>>>()
            {
              @Override
              public Iterable<TimelineObjectHolder<String, StorageAdapter>> apply(Interval input)
              {
                return timeline.lookup(input);
              }
            }
        )
        .transformCat(
            new Function<TimelineObjectHolder<String, StorageAdapter>, Iterable<QueryRunner<T>>>()
            {
              @Override
              public Iterable<QueryRunner<T>> apply(@Nullable final TimelineObjectHolder<String, StorageAdapter> holder)
              {
                if (holder == null) {
                  return null;
                }

                return FunctionalIterable
                    .create(holder.getObject())
                    .transform(
                        new Function<PartitionChunk<StorageAdapter>, QueryRunner<T>>()
                        {
                          @Override
                          public QueryRunner<T> apply(PartitionChunk<StorageAdapter> input)
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
                    );
              }
            }
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


    final VersionedIntervalTimeline<String, StorageAdapter> timeline = dataSources.get(query.getDataSource());

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
              public Iterable<QueryRunner<T>> apply(@Nullable SegmentDescriptor input)
              {
                final PartitionHolder<StorageAdapter> entry = timeline.findEntry(
                    input.getInterval(), input.getVersion()
                );

                if (entry == null) {
                  return null;
                }

                final PartitionChunk<StorageAdapter> chunk = entry.getChunk(input.getPartitionNumber());
                if (chunk == null) {
                  return null;
                }

                final StorageAdapter adapter = chunk.getObject();
                return Arrays.<QueryRunner<T>>asList(
                    buildAndDecorateQueryRunner(factory, toolChest, adapter, new SpecificSegmentSpec(input))
                );
              }
            }
        );

    return new FinalizeResultsQueryRunner<T>(toolChest.mergeResults(factory.mergeRunners(exec, adapters)), toolChest);
  }

  private <T> QueryRunner<T> buildAndDecorateQueryRunner(
      QueryRunnerFactory<T, Query<T>> factory,
      final QueryToolChest<T, Query<T>> toolChest,
      StorageAdapter adapter,
      QuerySegmentSpec segmentSpec
  )
  {
    return new SpecificSegmentQueryRunner<T>(
        new MetricsEmittingQueryRunner<T>(
            emitter,
            new Function<Query<T>, ServiceMetricEvent.Builder>()
            {
              @Override
              public ServiceMetricEvent.Builder apply(@Nullable Query<T> input)
              {
                return toolChest.makeMetricBuilder(input);
              }
            },
            new BySegmentQueryRunner<T>(
                adapter.getSegmentIdentifier(),
                adapter.getInterval().getStart(),
                factory.createRunner(adapter)
            )
        ),
        segmentSpec
    );
  }
}
