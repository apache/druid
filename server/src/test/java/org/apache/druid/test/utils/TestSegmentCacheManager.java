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

package org.apache.druid.test.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.TestSegmentUtils;
import org.apache.druid.segment.loading.AcquireSegmentAction;
import org.apache.druid.segment.loading.AcquireSegmentResult;
import org.apache.druid.segment.loading.NoopSegmentCacheManager;
import org.apache.druid.segment.loading.TombstoneSegmentizerFactory;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A local cache manager to test the bootstrapping and segment add/remove operations. It stubs only the necessary
 * methods to support these operations; any other method invoked will throw an exception from the base class,
 * {@link NoopSegmentCacheManager}.
 */
public class TestSegmentCacheManager extends NoopSegmentCacheManager
{
  private final List<DataSegment> cachedSegments;
  private final Map<SegmentId, ReferenceCountedSegmentProvider> referenceProviders;
  private final Map<SegmentId, DataSegment> segmentLookup;

  private final List<DataSegment> observedBootstrapSegments;
  private final List<DataSegment> observedSegments;
  private final Set<SegmentId> observedSegmentsRemovedFromCache;
  private final AtomicInteger observedShutdownBootstrapCount;

  public TestSegmentCacheManager()
  {
    this(ImmutableSet.of());
  }

  public TestSegmentCacheManager(final Set<DataSegment> segmentsToCache)
  {
    this.cachedSegments = ImmutableList.copyOf(segmentsToCache);
    this.referenceProviders = new ConcurrentHashMap<>();
    this.segmentLookup = new ConcurrentHashMap<>();

    // While inneficient, these CopyOnWriteArrayList objects greatly simplify meeting the thread
    // safety mandate from SegmentCacheManager. For testing, this should be ok.
    this.observedBootstrapSegments = new CopyOnWriteArrayList<>();
    this.observedSegments = new CopyOnWriteArrayList<>();
    this.observedSegmentsRemovedFromCache = ConcurrentHashMap.newKeySet();

    this.observedShutdownBootstrapCount = new AtomicInteger(0);
  }

  /**
   * Registers a pre-built segment, so that {@link #acquireSegment} and {@link #acquireCachedSegment}
   * will return this segment instead of creating one from the DataSegment's loadSpec.
   */
  public void registerSegment(final DataSegment dataSegment, final Segment segment)
  {
    segmentLookup.put(dataSegment.getId(), dataSegment);
    referenceProviders.put(dataSegment.getId(), ReferenceCountedSegmentProvider.of(segment));
  }

  @Override
  public boolean canHandleSegments()
  {
    return true;
  }

  @Override
  public List<DataSegment> getCachedSegments()
  {
    return cachedSegments;
  }

  @Override
  public void bootstrap(DataSegment segment, SegmentLazyLoadFailCallback loadFailed)
  {
    observedBootstrapSegments.add(segment);
    getSegmentInternal(segment);
  }

  @Override
  public void load(final DataSegment segment)
  {
    observedSegments.add(segment);
    getSegmentInternal(segment);
  }

  private ReferenceCountedSegmentProvider getSegmentInternal(final DataSegment segment)
  {
    segmentLookup.putIfAbsent(segment.getId(), segment);
    return referenceProviders.compute(
        segment.getId(),
        (id, existingProvider) -> {
          if (existingProvider == null) {
            final DataSegment s = segmentLookup.get(id);
            if (s.isTombstone()) {
              return ReferenceCountedSegmentProvider.of(TombstoneSegmentizerFactory.segmentForTombstone(s));
            } else {
              return ReferenceCountedSegmentProvider.of(
                  new TestSegmentUtils.SegmentForTesting(
                      s.getDataSource(),
                      (Interval) s.getLoadSpec().get("interval"),
                      MapUtils.getString(s.getLoadSpec(), "version")
                  )
              );
            }
          }
          return existingProvider;
        }
    );
  }

  @Override
  public Optional<Segment> acquireCachedSegment(SegmentId segmentId)
  {
    if (observedSegmentsRemovedFromCache.contains(segmentId)) {
      return Optional.empty();
    }
    final ReferenceCountedSegmentProvider provider = referenceProviders.get(segmentId);
    if (provider == null) {
      return Optional.empty();
    }
    return provider.acquireReference();
  }

  @Override
  public AcquireSegmentAction acquireSegment(DataSegment dataSegment)
  {
    if (observedSegmentsRemovedFromCache.contains(dataSegment.getId())) {
      return AcquireSegmentAction.missingSegment();
    }
    return new AcquireSegmentAction(
        () -> Futures.immediateFuture(AcquireSegmentResult.cached(getSegmentInternal(dataSegment))),
        null
    );
  }

  @Override
  public void shutdownBootstrap()
  {
    observedShutdownBootstrapCount.incrementAndGet();
  }

  @Override
  public void shutdown()
  {
    // do nothing
  }

  @Override
  public void storeInfoFile(DataSegment segment)
  {
    // do nothing
  }

  @Override
  public void removeInfoFile(DataSegment segment)
  {
    // do nothing
  }

  @Override
  public void drop(DataSegment segment)
  {
    getSegmentInternal(segment).close();
    observedSegmentsRemovedFromCache.add(segment.getId());
  }

  public List<DataSegment> getObservedBootstrapSegments()
  {
    return observedBootstrapSegments;
  }

  public List<DataSegment> getObservedSegments()
  {
    return observedSegments;
  }


  public Set<SegmentId> getObservedSegmentsRemovedFromCache()
  {
    return observedSegmentsRemovedFromCache;
  }

  public AtomicInteger getObservedShutdownBootstrapCount()
  {
    return observedShutdownBootstrapCount;
  }
}
